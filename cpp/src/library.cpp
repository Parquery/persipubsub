#include "library.h"

#include <iostream>

void hello() {
    std::cout << "Hello, World!" << std::endl;
}

namespace persipubsub {
    unsigned int MAX_READER_NUM = 1024;
    unsigned int MAX_DB_NUM = 1024;
    unsigned long MAX_DB_SIZE_BYTES = 32UL * 1024UL * 1024UL * 1024UL;

    // define all database names here
    char DATA_DB[] = "data_db";   // msg_id | data
    char PENDING_DB[] = "pending_db";  // msg_id | pending subscriber
    char META_DB[] = "meta_db";  // msg_id | metadata
    char QUEUE_DB[] = "queue_db";  // queue_pth | all queue data
} // namespace persipubsub