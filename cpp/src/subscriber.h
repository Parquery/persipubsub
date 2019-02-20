//
// Created by selim on 05.02.19.
//

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <vector>

#include "queue.h"

namespace persipubsub{
    namespace subscriber{
        /**
         *  Create Subscriber ready to receive messages.
         */
        class Subscriber{

        public:
            /**
             * Initialize class object.
             */
            Subscriber() {};

            /**
             * Initialize.
             *
             * @param identifier unique subscriber id
             * @param path to the queue
             */
            void init(std::vector<char> identifier, boost::filesystem::path path);

            /**
             * Receive messages from the publisher.
             *
             * @param timeout time waiting for a message. If none arrived until the timeout then None will be returned. (secs)
             * @param retries number of tries to check if a msg arrived in the queue
             * @return received message
             */
            char receive(int timeout, int retries);

            /**
             * Pops all messages until the most recent one and receive the latest.
             * Used in the case that a particular subscriber cares only about the very
             * last message and other subscribers care about all the messages in the
             * queue.
             * For another use case, when you only want to store the latest message
             * and all subscribers are interested only in the latest, then use
             * high water mark max_msgs_num = 1.
             *
             * @param timeout time waiting for a message. If none arrived until the timeout then None will be returned. (secs)
             * @param retries number of tries to check if a msg arrived in the queue
             * @return received message
             */
            char receive_to_top(int timeout, int retries);

            ~Subscriber();
        private:
            std::vector<char> identifier_;
            persipubsub::queue::Queue queue_;

            /**
             * Pop a message from the subscriber's lmdb.
             */
            void pop();

        }; // class Subscriber
    } // namespace subscriber
} // namespace persipubsub