//
// Created by selim on 05.02.19.
//

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <vector>

#include "queue.h"

namespace persipubsub {
    namespace control {
        /**
         * Store queue parameter in lmdb.
         *
         * @param max_reader_num max number of reader of the lmdb
         * @param max_db_num max number of named databases
         * @param max_db_size_bytes max size of the lmdb in bytes
         * @param env where parameters are stored
         */
        void set_queue_parameters(int max_reader_num, int max_db_num,
                                          int max_db_size_bytes, lmdb::env env);

        /**
         * Set high water mark values for queue.
         *
         * @param hwm high water mark values
         * @param env where values are stored
         */
        void set_hwm(persipubsub::queue::HighWaterMark hwm, lmdb::env env);

        /**
         * Set pruning strategy for queue.
         *
         * @param strategy pruning strategy
         * @param env where strategy are stored
         */
        void set_strategy(persipubsub::queue::Strategy strategy, lmdb::env env);

        /**
         * Add a subscriber and create its lmdb.
         *
         * @param identifier ID of the subscriber which should be added
         * @param env where subscriber is added
         */
        void _add_sub(char identifier, lmdb::env env);

        /**
         * Control and maintain one queue.
         */
        class Control {
        public:
            /**
             * Initialize control class.
             *
             * @param path to the queue.
             */
            Control(boost::filesystem::path path) path_(path) {};

            /**
             * Initialize control with a (re)initialized queue.
             *
             * @param subscriber_ids subscribers of the queue
             * @param max_readers max number of reader of the lmdb
             * @param max_size of the lmdb in bytes
             * @param high_water_mark limit of the queue
             * @param strategy used to prune queue
             */
            void init(std::vector<char> subscriber_ids, int max_readers,
                    int max_size,
                    persipubsub::queue::HighWaterMark high_water_mark,
                    persipubsub::queue::Strategy strategy);

            /**
             * Check if queue is initialized.
             *
             * @return is initialized when all values for the given keys are set
             */
            bool check_queue_is_initialized();

            /**
             * Clear all subscriber and delete all messages for queue.
             */
            void clear_all_subscribers();

            /**
             * Prune all dangling messages from the lmdb.
             */
            void prune_dangling_messages();

            ~Control();
        private:
            boost::filesystem::path path_;
            persipubsub::queue::Queue queue_;
            std::vector<char> subscriber_ids_;

            /**
             * Reinitialize the queue which is maintained by the control.
             */
            void reinitialize_queue();

            /**
             * Initialize queue.
             *
             * @param subscriber_ids subscribers of the queue
             * @param max_readers max number of reader of the lmdb
             * @param max_size of the lmdb in bytes
             * @param high_water_mark limit of the queue
             * @param strategy used to prune queue
             */
            void initialize_queue(std::vector<char> subscriber_ids,
                    int max_readers, int max_size,
                    persipubsub::queue::HighWaterMark high_water_mark,
                    persipubsub::queue::Strategy strategy);

            /**
             * Prune all messages of a subscriber.
             *
             * @param identifier ID of the subscriber of which all messages should be pruned
             */
            void prune_all_messages_for(std::vector<char> identifier);

            /**
             * Remove a subscriber and delete all its messages.
             *
             * @param identifier ID of the subscriber which should be removed
             * @param env from where subscriber is removed
             */
            void remove_sub(std::vector<char> identifier, lmdb::env env);

        }; // class Control
    } // namespace control
} // namespace persipubsub