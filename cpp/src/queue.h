// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <vector>

namespace persipubsub {
    namespace queue {

        class Queue;

        static unsigned int MSG_TIMEOUT_SECS = 500;
        static unsigned int MAX_MSGS_NUM = 1024 * 64;
        static unsigned long HWM_LMDB_SIZE_BYTES = 30UL * 1024UL * 1024UL * 1024UL;

        /**
         * Store possible strategies.
         */
        enum Strategy {
            prune_first = 0,
            prune_last
        };

        /**
         * Parse overflow strategy.
         *
         * @param strategy Strategy stored in config
         * @return set overflow strategy
         */
        Strategy parse_strategy(const std::string& strategy);

        /**
         * Store high water mark limits.
         */
        struct HighWaterMark{
            /**
             * Initialize.
             *
             * @param msg_timeout_secs time after which msg is classified as dangling msg (secs)
             * @param max_masgs_num maximal amount of msg
             * @param hwm_lmdb_size_bytes high water mark for total size of lmdb (bytes)
             */
            unsigned int msg_timeout_secs_ = MSG_TIMEOUT_SECS;
            unsigned int max_msgs_num_ = MAX_MSGS_NUM;
            unsigned long hwm_lmdb_size_bytes_ = HWM_LMDB_SIZE_BYTES;

            HighWaterMark() = default;

            HighWaterMark(const unsigned int msg_timeout_secs,
                    const unsigned int max_msgs_num,
                    const unsigned long hwm_lmdb_size) : msg_timeout_secs_(msg_timeout_secs), max_msgs_num_(max_msgs_num), hwm_lmdb_size_bytes_(hwm_lmdb_size) {}
        };

        /**
         * Initialize the queue; the queue directory is assumed to exist.
         *
         * @param queue_dir where the queue is stored
         * @param max_reader_num maximal number of reader
         * @param max_db_num maximal number of databases
         * @param max_db_size_bytes maximal size of database (bytes)
         * @return Load or if needed create LMDB queue from directory
         */
        lmdb::env initialize_environment(const boost::filesystem::path& queue_dir, const unsigned int max_reader_num, const unsigned int max_db_num, const unsigned long max_db_size_bytes);

        /**
         * Prune all dangling messages for subscribers of a queue from lmdb.
         *
         * @param queue of which dangling messages should be pruned
         * @param subscriber_ids subscribers of which dangling messages should be pruned
         */
        void prune_dangling_messages_for(const persipubsub::queue::Queue& queue, const std::vector<std::string>& subscriber_ids);

        /**
         * Queue messages persistently from many publishers for many subscribers.
         */
        class Queue{

        public:
            /**
             * Initialize class object.
             */
            Queue() : path_(),
                    env_(nullptr), hwm_(), strategy_(),
                    subscriber_ids_() {}

            /**
             * Initialize the queue.
             *
             * @param config_pth path to the  JSON config file
             * @param queue_dir where the queue is stored
             * @param max_reader_num maximal number of reader
             * @param max_db_num maximal number of databases
             * @param max_db_size_bytes maximal size of database (bytes)
             */
            void init(const boost::filesystem::path& path, lmdb::env env= nullptr);

            /**
             * Put message to lmdb queue.
             *
             * @param msg message send from publisher to subscribers
             * @param subscriber_ids List of subscribers
             */
            void put(const std::string& msg, const std::vector<std::string>& subscriber_ids) const;

            /**
             * Put many message to lmdb queue.
             *
             * @param msgs  messages send from publisher to subscribers
             * @param subscriber_ids List of subscribers
             */
            void put_many_flush_once(const std::vector<std::string>& msgs, const std::vector<std::string>& subscriber_ids) const;

            /**
             * Peek at next message in lmdb queue.
             *
             * Load from LMDB queue into memory and process msg afterwards.
             * @param identifier Subscriber ID
             * @param message peek on the message
             */
            void front(const std::string& identifier, std::string* msg) const;

            /**
             * Remove msg from the subscriber's queue and reduce pending subscribers.
             *
             * @param identifier Subscriber ID
             */
            void pop(const std::string& identifier) const;

            /**
             * Prune dangling messages in the queue.
             */
            void prune_dangling_messages() const;

            /**
             * Check current lmdb size in bytes.
             *
             * Check size of data database by approximating size with multiplying page size with number of pages.
             * @return  data database size in bytes
             */
            unsigned long check_current_lmdb_size() const;

            /**
             * Count number of messages in database.
             *
             * Count number of messages stored in meta database.
             *
             * @return number of messages in database
             */
            unsigned int count_msgs() const;

            /**
             * Clean database when needed.
             */
            void vacuum() const;

            /**
             * Prune one half of the messages stored.
             *
             * Depending on the strategy the first or the last will be deleted.
             */
            void prune_messages() const;

            boost::filesystem::path path_;
            lmdb::env env_;
            HighWaterMark hwm_;
            persipubsub::queue::Strategy strategy_;
            std::vector<std::string> subscriber_ids_;

            ~Queue();

        private:

        }; // class Queue

    }  // namespace queue
} // namespace persipubsub
