//
// Created by selim on 01.02.19.
//

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <boost/algorithm/string/classification.hpp> // Include boost::for is_any_of
#include <boost/algorithm/string/split.hpp> // Include for boost::split
#include <vector>
#include "queue.h"

namespace persipubsub {
    // TODO read when it is appropriate to use extern

    extern unsigned int MAX_READER_NUM;
    extern unsigned int MAX_DB_NUM;
    extern unsigned long MAX_DB_SIZE_BYTES;

    // define all database names here
    extern char DATA_DB[];   // msg_id | data
    extern char PENDING_DB[];  // msg_id | pending subscriber
    extern char META_DB[];  // msg_id | metadata
    extern char QUEUE_DB[];  // queue_pth | all queue data

    extern char HWM_DB_SIZE_BYTES_KEY[];
    extern char MAX_MSGS_NUM_KEY[];
    extern char MSG_TIMEOUT_SECS_KEY[];
    extern char STRATEGY_KEY[];
    extern char SUBSCRIBER_IDS_KEY[];

    struct QueueData{
        unsigned int msg_timeout_secs_;
        unsigned int max_msgs_num_;
        unsigned long hwm_db_size_bytes_;
        persipubsub::queue::Strategy strategy_;
        std::vector<std::string> subscriber_ids_;

        QueueData(unsigned int msg_timeout_secs, unsigned int max_msgs_num,
                unsigned long hwm_db_size_bytes,
                persipubsub::queue::Strategy strategy,
                std::vector<std::string> subscriber_ids) :
                msg_timeout_secs_(msg_timeout_secs),
                max_msgs_num_(max_msgs_num),
                hwm_db_size_bytes_(hwm_db_size_bytes), strategy_(strategy),
                subscriber_ids_(std::move(subscriber_ids)) {};
    };
    QueueData lookup_queue_data(const lmdb::env& env);
} // namespace persipubsub