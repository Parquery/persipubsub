#include "library.h"

#include <iostream>

namespace persipubsub {
    unsigned int MAX_READER_NUM = 1024;
    unsigned int MAX_DB_NUM = 1024;
    unsigned long MAX_DB_SIZE_BYTES = 32UL * 1024UL * 1024UL * 1024UL;

    // define all database names here
    char DATA_DB[] = "data_db";   // msg_id | data
    char PENDING_DB[] = "pending_db";  // msg_id | pending subscriber
    char META_DB[] = "meta_db";  // msg_id | metadata
    char QUEUE_DB[] = "queue_db";  // queue_pth | all queue data

    char HWM_DB_SIZE_BYTES_KEY[] = "hwm_db_size_bytes";
    char MAX_MSGS_NUM_KEY[] = "max_msgs_num";
    char MSG_TIMEOUT_SECS_KEY[] = "msg_timeout_secs";
    char STRATEGY_KEY[] = "strategy";
    char SUBSCRIBER_IDS_KEY[] = "subscriber_ids";

    QueueData lookup_queue_data(const lmdb::env& env) {
        auto rtxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
        auto queue_dbi = lmdb::dbi::open(rtxn, QUEUE_DB);

        lmdb::val size_key(HWM_DB_SIZE_BYTES_KEY), size;

        queue_dbi.get(rtxn, size_key, size);
        unsigned long hwm_db_size_bytes = std::stoul(size.data());


        lmdb::val msgs_key(MAX_MSGS_NUM_KEY), msgs_num;
        queue_dbi.get(rtxn, msgs_key, msgs_num);
        unsigned int max_msgs_num = std::stoi(msgs_num.data());

        lmdb::val timeout_key(MSG_TIMEOUT_SECS_KEY), timeout;
        queue_dbi.get(rtxn, timeout_key, timeout);
        unsigned int msg_timeout_secs = std::stoi(timeout.data());

        lmdb::val strategy_key(STRATEGY_KEY), strategy_val;
        queue_dbi.get(rtxn, strategy_key, strategy_val);
        std::string strategy_str = strategy_val.data();
        queue::Strategy strategy = queue::parse_strategy(strategy_str);

        lmdb::val subscriber_ids_key(SUBSCRIBER_IDS_KEY), subscriber_ids_val;
        queue_dbi.get(rtxn, subscriber_ids_key, subscriber_ids_val);
        std::string subscriber_ids_str = subscriber_ids_val.data();
        std::vector<std::string> subscriber_ids;

        boost::split(subscriber_ids, subscriber_ids_str, boost::is_any_of(" "), boost::token_compress_on);

        return QueueData(msg_timeout_secs, max_msgs_num, hwm_db_size_bytes, strategy, subscriber_ids);
    }
} // namespace persipubsub