// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <boost/algorithm/string/classification.hpp> // Include boost::for is_any_of
#include <boost/algorithm/string/split.hpp> // Include for boost::split
#include <vector>
#include "queue.h"

namespace persipubsub {
    static unsigned int MAX_READER_NUM = 1024;
    static unsigned int MAX_DB_NUM = 1024;
    static unsigned long MAX_DB_SIZE_BYTES = 32UL * 1024UL * 1024UL * 1024UL;

    // define all database names here
    static char DATA_DB[] = "data_db";   // msg_id | data
    static char PENDING_DB[] = "pending_db";  // msg_id | pending subscriber
    static char META_DB[] = "meta_db";  // msg_id | metadata
    static char QUEUE_DB[] = "queue_db";  // queue_pth | all queue data

    static char HWM_DB_SIZE_BYTES_KEY[] = "hwm_db_size_bytes";
    static char MAX_MSGS_NUM_KEY[] = "max_msgs_num";
    static char MSG_TIMEOUT_SECS_KEY[] = "msg_timeout_secs";
    static char STRATEGY_KEY[] = "strategy";
    static char SUBSCRIBER_IDS_KEY[] = "subscriber_ids";

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