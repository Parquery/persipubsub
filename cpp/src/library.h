//
// Created by selim on 01.02.19.
//

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <vector>

namespace persipubsub {
    extern unsigned int MAX_READER_NUM;
    extern unsigned int MAX_DB_NUM;
    extern unsigned long MAX_DB_SIZE_BYTES;

    // define all database names here
    extern char DATA_DB[];   // msg_id | data
    extern char PENDING_DB[];  // msg_id | pending subscriber
    extern char META_DB[];  // msg_id | metadata
    extern char QUEUE_DB[];  // queue_pth | all queue data
} // namespace persipubsub