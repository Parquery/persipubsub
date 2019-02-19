// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE TestQueue
#include <boost/test/unit_test.hpp>

#include <filesystem.h>
#include <prettyprint.hpp>
#include <unique_resource.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "../src/queue.h"
#include "../src/library.h"

namespace fs = boost::filesystem;

class TmpDir{
public:
    TmpDir(){
        path_ = fs::unique_path();
        fs::create_directories(path_);
    }

    ~TmpDir(){
        fs::remove_all(path_);
    }
    fs::path path_;
private:

}; // class TmpDir


void setup_queue(const lmdb::txn& wtxn, const unsigned int msg_timeout_secs,
        const unsigned int max_msgs_num, const unsigned long hwm_lmdb_size,
        const std::string& strategy, const std::string& sub_ids){
    auto queue_dbi = lmdb::dbi::open(wtxn, persipubsub::QUEUE_DB, MDB_CREATE);

    lmdb::val msg_timeout_secs_key(persipubsub::MSG_TIMEOUT_SECS_KEY);
    lmdb::val msg_timeout_secs_val(boost::lexical_cast<std::string>(msg_timeout_secs));
    queue_dbi.put(wtxn, msg_timeout_secs_key, msg_timeout_secs_val);

    lmdb::val max_msgs_num_key(persipubsub::MAX_MSGS_NUM_KEY);
    lmdb::val max_msgs_num_val(boost::lexical_cast<std::string>(max_msgs_num));
    queue_dbi.put(wtxn, max_msgs_num_key, max_msgs_num_val);

    lmdb::val hwm_lmdb_size_key(persipubsub::HWM_DB_SIZE_BYTES_KEY);
    lmdb::val hwm_lmdb_size_val(boost::lexical_cast<std::string>(hwm_lmdb_size));
    queue_dbi.put(wtxn, hwm_lmdb_size_key, hwm_lmdb_size_val);

    lmdb::val strategy_key(persipubsub::STRATEGY_KEY);
    lmdb::val strategy_val(strategy);
    queue_dbi.put(wtxn, strategy_key, strategy_val);

    lmdb::val subscriber_ids_key(persipubsub::SUBSCRIBER_IDS_KEY);
    lmdb::val subscriber_ids_val(sub_ids);
    queue_dbi.put(wtxn, subscriber_ids_key, subscriber_ids_val);
}


BOOST_AUTO_TEST_CASE(test_initialize_environment){

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024, 1024, 32UL * 1024UL * 1024UL * 1024UL);

    auto rtxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbi = lmdb::dbi::open(rtxn, nullptr);

    MDB_envinfo info;
    lmdb::env_info(env, &info);

    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(info.me_last_txnid));
    BOOST_REQUIRE_EQUAL("1", boost::lexical_cast<std::string>(info.me_last_pgno));
    BOOST_REQUIRE_EQUAL("1024", boost::lexical_cast<std::string>(info.me_maxreaders));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(info.me_mapaddr));
    BOOST_REQUIRE_EQUAL("34359738368", boost::lexical_cast<std::string>(info.me_mapsize));
    BOOST_REQUIRE_EQUAL("1", boost::lexical_cast<std::string>(info.me_numreaders));

    MDB_stat stat = dbi.stat(rtxn);

    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_branch_pages));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_entries));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_depth));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_leaf_pages));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_overflow_pages));
    BOOST_REQUIRE_EQUAL("4096", boost::lexical_cast<std::string>(stat.ms_psize));

    rtxn.abort();
}

BOOST_AUTO_TEST_CASE(test_put_to_single_subscriber) {

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024,
                                                               1024,
                                                               32UL * 1024UL *
                                                               1024UL * 1024UL);
    // todo ask Adam if '.' will be a problem.
    std::string msg = "I'm a message.\n"; // TODO read about carriage return, /0 and other funky issues
    std::string sub_ids = "sub";
    std::string strategy = "prune_first";

    unsigned int msg_timeout_secs = 500;
    unsigned int max_msgs_num = 1000;
    unsigned long hwm_lmdb_size = 30UL * 1024UL * 1024UL * 1024UL;

    auto wtxn = lmdb::txn::begin(env);
    auto sub_dbi = lmdb::dbi::open(wtxn, sub_ids.c_str(), MDB_CREATE);

    setup_queue(wtxn, msg_timeout_secs, max_msgs_num, hwm_lmdb_size, strategy, sub_ids);

    wtxn.commit();
    // The Environment is closed automatically. For testing purpose the
    // environment is here forcefully closed.
    env.close();

    persipubsub::queue::Queue queue = persipubsub::queue::Queue();
    queue.init(queue_dir);

    std::vector<std::string> subs;
    subs.push_back(sub_ids);
    queue.put(msg, subs);

    std::string result;
    queue.front(sub_ids, &result);

    BOOST_REQUIRE_EQUAL(msg, result);

}

BOOST_AUTO_TEST_CASE(test_put_multiple_subscriber) {

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024,
                                                               1024,
                                                               32UL * 1024UL *
                                                               1024UL * 1024UL);
    // todo ask Adam if '.' will be a problem.
    std::string msg = "I'm a message.\n";
    std::string sub_ids = "sub1 sub2";
    std::string strategy = "prune_first";

    unsigned int msg_timeout_secs = 500;
    unsigned int max_msgs_num = 1000;
    unsigned long hwm_lmdb_size = 30UL * 1024UL * 1024UL * 1024UL;

    auto wtxn = lmdb::txn::begin(env);
    auto sub1_dbi = lmdb::dbi::open(wtxn, "sub1", MDB_CREATE);
    auto sub2_dbi = lmdb::dbi::open(wtxn, "sub2", MDB_CREATE);

    setup_queue(wtxn, msg_timeout_secs, max_msgs_num, hwm_lmdb_size, strategy, sub_ids);

    wtxn.commit();
    // The Environment is closed automatically. For testing purpose the
    // environment is here forcefully closed.
    env.close();

    persipubsub::queue::Queue queue = persipubsub::queue::Queue();
    queue.init(queue_dir);

    std::vector<std::string> subs;
    subs.emplace_back("sub1");
    subs.emplace_back("sub2");
    queue.put(msg, subs);

    std::string result1;
    queue.front("sub1", &result1);
    BOOST_REQUIRE_EQUAL(msg, result1);
    std::string result2;
    queue.front("sub2", &result2);
    BOOST_REQUIRE_EQUAL(msg, result2);
}

BOOST_AUTO_TEST_CASE(test_put_many) {

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024,
                                                               1024,
                                                               32UL * 1024UL *
                                                               1024UL * 1024UL);
    // todo ask Adam if '.' will be a problem.
    std::string msg = "I'm a message.\n";
    std::string sub_ids = "sub";
    std::string strategy = "prune_first";

    unsigned int msg_timeout_secs = 500;
    unsigned int max_msgs_num = 1000;
    unsigned long hwm_lmdb_size = 30UL * 1024UL * 1024UL * 1024UL;

    auto wtxn = lmdb::txn::begin(env);
    auto sub_dbi = lmdb::dbi::open(wtxn, sub_ids.c_str(), MDB_CREATE);

    setup_queue(wtxn, msg_timeout_secs, max_msgs_num, hwm_lmdb_size, strategy,
                sub_ids);

    wtxn.commit();
    // The Environment is closed automatically. For testing purpose the
    // environment is here forcefully closed.
    env.close();

    persipubsub::queue::Queue queue = persipubsub::queue::Queue();
    queue.init(queue_dir);

    std::vector<std::string> subs;
    subs.push_back(sub_ids);

    std::vector<std::string> msgs;
    unsigned int test_num_msgs = 10;
    for (int num_msgs = test_num_msgs; num_msgs > 0; num_msgs--)
        msgs.push_back(msg);

    queue.put_many_flush_once(msgs, subs);

    std::string result;
    queue.front(sub_ids, &result);

    BOOST_REQUIRE_EQUAL(msg, result);
    BOOST_REQUIRE_EQUAL(test_num_msgs, queue.count_msgs());
}

BOOST_AUTO_TEST_CASE(test_pop) {

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024,
                                                               1024,
                                                               32UL * 1024UL *
                                                               1024UL * 1024UL);
    // todo ask Adam if '.' will be a problem.
    std::string msg = "I'm a message.\n";
    std::string sub_ids = "sub";
    std::string strategy = "prune_first";

    unsigned int msg_timeout_secs = 500;
    unsigned int max_msgs_num = 1000;
    unsigned long hwm_lmdb_size = 30UL * 1024UL * 1024UL * 1024UL;

    auto wtxn = lmdb::txn::begin(env);
    auto sub_dbi = lmdb::dbi::open(wtxn, sub_ids.c_str(), MDB_CREATE);

    setup_queue(wtxn, msg_timeout_secs, max_msgs_num, hwm_lmdb_size, strategy,
                sub_ids);

    wtxn.commit();
    // The Environment is closed automatically. For testing purpose the
    // environment is here forcefully closed.
    env.close();

    persipubsub::queue::Queue queue = persipubsub::queue::Queue();
    queue.init(queue_dir);

    std::vector<std::string> subs;
    subs.push_back(sub_ids);
    queue.put(msg, subs);

    std::string result;
    queue.front(sub_ids, &result);

    BOOST_REQUIRE_EQUAL(msg, result);

    queue.pop(sub_ids);
    std::string empty;
    queue.front(sub_ids, &empty);
    BOOST_REQUIRE_EQUAL("", empty.c_str());
}