// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

#include "queue.h"

//
// Created by selim on 01.02.19.
//

#include "library.h"
#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <vector>
#include <map>
#include <time.h>
#include <set>

namespace fs = boost::filesystem;

std::map<std::string, persipubsub::queue::Strategy> strategyMap;

constexpr char const_prune_first[] = "prune_first";
constexpr char const_prune_last[] = "prune_last";

persipubsub::queue::Strategy persipubsub::queue::parse_strategy(const std::string &strategy) {
    strategyMap[const_prune_first] = persipubsub::queue::Strategy::prune_first;
    strategyMap[const_prune_last] = persipubsub::queue::Strategy::prune_last;

    auto result = strategyMap.find(strategy);
    if (result == strategyMap.end())
        throw std::runtime_error("Unknown strategy: " + strategy);
    return result->second;


}


lmdb::env persipubsub::queue::initialize_environment(const fs::path& queue_dir,
                                 const unsigned int max_reader_num,
                                 const unsigned int max_db_num,
                                 const unsigned long max_db_size_bytes) {

    if (not fs::exists(queue_dir))
        throw std::runtime_error("The queue directory does not exist: " +
                                 queue_dir.filename().string());

    auto env = lmdb::env::create();
    env.set_mapsize(max_db_size_bytes);
    env.set_max_dbs(max_db_num);
    env.set_max_readers(max_reader_num);
    env.open(queue_dir.filename().string().c_str(), 0, 0664);
    return env;

}


void persipubsub::queue::prune_dangling_messages_for(const persipubsub::queue::Queue& queue,
                                 const std::vector<std::string> &subscriber_ids){

    /*todo check mistake:
     *  terminate called after throwing an instance of 'lmdb::runtime_error'
        what():  mdb_txn_commit: Invalid argument
        unknown location(0): fatal error in "test_put_to_single_subscriber": signal: SIGABRT (application abort requested)
        /home/selim/workspace/pqry/persipubsub/cpp/test/test_queue.cpp(99): last checkpoint
        terminate called recursively
     */

    auto wtxn = persipubsub::WriteTransaction(queue.env_).wtxn_;
    //auto wtxn = lmdb::txn::begin(queue.env_);
    auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB);
    auto meta_dbi = lmdb::dbi::open(wtxn, persipubsub::META_DB);
    auto data_dbi = lmdb::dbi::open(wtxn, persipubsub::DATA_DB);

    // Definition of dangling messages:
    //   - having no pending subscribers
    //   - exists longer than timeout allows
    std::set<lmdb::val> msgs_to_delete;

    auto pending_cursor = lmdb::cursor::open(wtxn, pending_dbi);

    lmdb::val pending_key, pending_subscribers_num;
    while (pending_cursor.get(pending_key, pending_subscribers_num, MDB_NEXT)) {
        if (std::stoi(pending_subscribers_num.data()) == 0)
            msgs_to_delete.insert(std::move(pending_key));
    }
    pending_cursor.close();

    time_t timer;
    time(&timer);
    const auto timestamp_now = boost::numeric_cast<int>(timer);

    std::set<lmdb::val> msgs_to_delete_timeout;

    auto meta_cursor = lmdb::cursor::open(wtxn, pending_dbi);

    lmdb::val meta_key, timestamp;
    while (meta_cursor.get(meta_key, timestamp, MDB_NEXT)) {
        if (timestamp_now - std::stoi(timestamp.data()) > queue.hwm_.msg_timeout_secs_)
            msgs_to_delete_timeout.insert(std::move(meta_key));
    }
    meta_cursor.close();

    for (const auto& timeout_msg: msgs_to_delete_timeout){
        lmdb::val new_timeout_msg((timeout_msg).data());
        msgs_to_delete.insert(std::move(new_timeout_msg));
    }

    for (const auto& delete_it: msgs_to_delete){
        pending_dbi.del(wtxn, delete_it);
        meta_dbi.del(wtxn, delete_it);
        data_dbi.del(wtxn, delete_it);
    }

    for (const auto& sub_it: queue.subscriber_ids_){
        auto sub_dbi = lmdb::dbi::open(wtxn, (sub_it).c_str());
        for (const auto& key_it: msgs_to_delete_timeout){
            sub_dbi.del(wtxn, key_it);
        }
    }

    //todo check if needed
    //wtxn.commit();
}

// todo add transaction and cursor classes
void persipubsub::queue::Queue::init(const boost::filesystem::path& path, lmdb::env env) {
    path_ = path;

    if (env)
        env_ = std::move(env);
    else
        env_ = persipubsub::queue::initialize_environment(path, persipubsub::MAX_READER_NUM, persipubsub::MAX_DB_NUM, persipubsub::MAX_DB_SIZE_BYTES);

    {
        auto wtxn = lmdb::txn::begin(env_);

        auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB, MDB_CREATE);
        auto meta_dbi = lmdb::dbi::open(wtxn, persipubsub::META_DB, MDB_CREATE);
        // lmdb::dbi::open(wtxn, persipubsub::META_DB, MDB_CREATE); // TODO ask marko how to deal with it
        auto data_dbi = lmdb::dbi::open(wtxn, persipubsub::DATA_DB, MDB_CREATE);
        auto queue_dbi = lmdb::dbi::open(wtxn, persipubsub::QUEUE_DB, MDB_CREATE);

        wtxn.commit(); // TODO this will not be needed, jumping out of scope
    }

    persipubsub::QueueData queue_data = persipubsub::lookup_queue_data(env_);

    hwm_ = HighWaterMark(queue_data.msg_timeout_secs_,
            queue_data.max_msgs_num_, queue_data.hwm_db_size_bytes_);

    strategy_ = queue_data.strategy_;

    subscriber_ids_ = std::move(queue_data.subscriber_ids_);
}


void persipubsub::queue::Queue::put(const std::string& msg, const std::vector<std::string> &subscriber_ids) const{

    vacuum();

    time_t timer;
    time(&timer);
    const auto time = boost::lexical_cast<std::string>(timer);
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    const std::string tmp = boost::uuids::to_string(uuid);

    std::string key = (time + tmp);
    lmdb::val msg_id(key.c_str());

    auto wtxn = lmdb::txn::begin(env_);

    auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB);
    lmdb::val pending_subs((boost::lexical_cast<std::string>(subscriber_ids.size())).c_str());
    pending_dbi.put(wtxn, msg_id, pending_subs);

    auto meta_dbi = lmdb::dbi::open(wtxn, persipubsub::META_DB);
    lmdb::val time_val(time);
    meta_dbi.put(wtxn, msg_id, time_val);

    auto data_dbi = lmdb::dbi::open(wtxn, persipubsub::DATA_DB);
    lmdb::val data_val(msg.c_str());
    data_dbi.put(wtxn, msg_id, data_val);
    for (const auto& it: subscriber_ids) {
        auto sub_dbi = lmdb::dbi::open(wtxn, (it).c_str());
        lmdb::val sub_data("");
        sub_dbi.put(wtxn, msg_id, sub_data);
    }

    wtxn.commit();

}

void persipubsub::queue::Queue::put_many_flush_once(const std::vector<std::string> &msgs,
                         const std::vector<std::string> &subscriber_ids) const {
    vacuum();

    time_t timer;
    time(&timer);
    const auto time = boost::lexical_cast<std::string>(timer);

    auto wtxn = lmdb::txn::begin(env_);

    auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB);
    auto meta_dbi = lmdb::dbi::open(wtxn, persipubsub::META_DB);
    auto data_dbi = lmdb::dbi::open(wtxn, persipubsub::DATA_DB);

    std::vector<lmdb::dbi> sub_dbis;
    for (const auto& it: subscriber_ids) {
        auto sub_dbi = lmdb::dbi::open(wtxn, (it).c_str());
        sub_dbis.push_back(std::move(sub_dbi));
    }

    for (const auto& msg: msgs) {

        boost::uuids::uuid uuid = boost::uuids::random_generator()();
        const std::string tmp = boost::uuids::to_string(uuid);
        std::string key = (time + tmp);
        lmdb::val msg_id(key.c_str());

        lmdb::val pending_subs((boost::lexical_cast<std::string>(subscriber_ids.size())).c_str());
        pending_dbi.put(wtxn, msg_id, pending_subs);
        lmdb::val time_val(time);
        meta_dbi.put(wtxn, msg_id, time_val);
        lmdb::val data_val(msg.c_str());
        data_dbi.put(wtxn, msg_id, data_val);

        for (auto& sub_dbi_it: sub_dbis) {
            lmdb::val sub_data("");
            sub_dbi_it.put(wtxn, msg_id, sub_data);
        }
    }
    wtxn.commit();

}

void persipubsub::queue::Queue::front(const std::string& identifier, std::string* msg) const{

    auto rtxn = lmdb::txn::begin(env_, nullptr, MDB_RDONLY);
    auto sub_dbi = lmdb::dbi::open(rtxn, identifier.c_str());
    auto cursor = lmdb::cursor::open(rtxn, sub_dbi);
    lmdb::val key, value;
    if (cursor.get(key, value, MDB_FIRST)) {
        auto data_dbi = lmdb::dbi::open(rtxn, persipubsub::DATA_DB);
        auto data_cursor = lmdb::cursor::open(rtxn, data_dbi);
        lmdb::val data_value;

        bool found =  data_cursor.get(key, data_value, MDB_FIRST);
        if (found){
            std::string tmp_str = data_value.data();
            std::swap(*msg, tmp_str);
        }
        else{
            cursor.close();
            rtxn.abort();
            throw std::runtime_error("Data not found");
        }
    }
    cursor.close();
    rtxn.abort();
}

void persipubsub::queue::Queue::pop(const std::string& identifier) const {
    auto wtxn = lmdb::txn::begin(env_);
    auto sub_dbi = lmdb::dbi::open(wtxn, identifier.c_str());
    auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB);

    auto cursor = lmdb::cursor::open(wtxn, sub_dbi);

    lmdb::val key, value;

    if (cursor.get(key, value, MDB_FIRST)) {
        sub_dbi.del(wtxn, key);
        lmdb::val pending_value;
        pending_dbi.get(wtxn, key, pending_value);
        auto pending_num = boost::numeric_cast<unsigned int>(std::stoi(pending_value.data()));
        pending_num--;
        lmdb::val pending_new_value((boost::lexical_cast<std::string>(pending_num)).c_str());
        pending_dbi.put(wtxn, key, pending_new_value);
        cursor.close();
        wtxn.commit();
    }
    else {
        cursor.close();
        wtxn.commit();
        throw std::runtime_error("No message to pop");
    }
}


void persipubsub::queue::Queue::prune_dangling_messages() const {
    persipubsub::queue::prune_dangling_messages_for(*this, subscriber_ids_);
}


// todo size_t instead of long?
unsigned long persipubsub::queue::Queue::check_current_lmdb_size() const{
    auto rtxn = lmdb::txn::begin(env_, nullptr, MDB_RDONLY);
    auto data_dbi = lmdb::dbi::open(rtxn, persipubsub::DATA_DB);

    MDB_stat data_stat = data_dbi.stat(rtxn);

    // todo check overflow, boost::numeric_cast to unsigned long?
    unsigned long lmdb_size_bytes = data_stat.ms_psize * (data_stat.ms_branch_pages + data_stat.ms_leaf_pages + data_stat.ms_overflow_pages);

    rtxn.abort();

    return lmdb_size_bytes;

}


unsigned int persipubsub::queue::Queue::count_msgs() const {
    auto rtxn = lmdb::txn::begin(env_, nullptr, MDB_RDONLY);
    auto meta_dbi = lmdb::dbi::open(rtxn, persipubsub::META_DB);

    auto msgs_num = boost::numeric_cast<unsigned int>(meta_dbi.stat(rtxn).ms_entries);
    rtxn.abort();
    return msgs_num;
}


void persipubsub::queue::Queue::vacuum() const {
    prune_dangling_messages();

    unsigned int msgs_num = count_msgs();

    if (msgs_num >= hwm_.max_msgs_num_)
        prune_messages();
    unsigned long lmdb_size_bytes = check_current_lmdb_size();
    if (lmdb_size_bytes >= hwm_.hwm_lmdb_size_bytes_)
        prune_messages();
}


void persipubsub::queue::Queue::prune_messages() const {

    std::set<lmdb::val> messages_to_delete;

    auto rtxn = lmdb::txn::begin(env_, nullptr, MDB_RDONLY);
    auto rmeta_dbi = lmdb::dbi::open(rtxn, persipubsub::META_DB);
    auto entries = boost::numeric_cast<unsigned int>(rmeta_dbi.stat(rtxn).ms_entries);

    auto cursor = lmdb::cursor::open(rtxn, rmeta_dbi);
    lmdb::val key, value;

    if (strategy_ == persipubsub::queue::Strategy::prune_first){
        cursor.get(key, value, MDB_FIRST);
        for (unsigned int counter = 0; counter<=(entries/2); counter++){
            messages_to_delete.insert(std::move(key));
            cursor.get(key, value, MDB_NEXT);
        }
    }
    else if (strategy_ == persipubsub::queue::Strategy::prune_last){
        cursor.get(key, value, MDB_LAST);
        for (unsigned int counter = 0; counter<=(entries/2); counter++){
            messages_to_delete.insert(std::move(key));
            cursor.get(key, value, MDB_PREV);
        }
    }
    else {
        cursor.close();
        rtxn.abort();
        throw std::runtime_error("Pruning strategy not set.");
    }

    cursor.close();
    rtxn.abort();

    auto wtxn = lmdb::txn::begin(env_);
    auto pending_dbi = lmdb::dbi::open(wtxn, persipubsub::PENDING_DB);
    auto meta_dbi = lmdb::dbi::open(wtxn, persipubsub::META_DB);
    auto data_dbi = lmdb::dbi::open(wtxn, persipubsub::DATA_DB);

    std::vector<lmdb::dbi> dbis;
    dbis.push_back(std::move(pending_dbi));
    dbis.push_back(std::move(meta_dbi));
    dbis.push_back(std::move(data_dbi));

    for (const auto& sub_it: subscriber_ids_){
        auto sub_dbi = lmdb::dbi::open(wtxn, (sub_it).c_str());
        dbis.push_back(std::move(sub_dbi));
    }

    for (const auto& msg_it: messages_to_delete) {
        for (auto& db_it: dbis){
            db_it.del(wtxn, msg_it);
        }
    }

    wtxn.commit();
}

persipubsub::queue::Queue::~Queue() {}

