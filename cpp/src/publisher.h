//
// Created by selim on 05.02.19.
//

#pragma once

#include <lmdb++.h>
#include <boost/filesystem/path.hpp>
#include <vector>

#include "queue.h"

namespace persipubsub{
    namespace publisher{
        /**
         * Create Publisher ready to send messages.
         */
        class Publisher{

        public:
            Publisher() {};

            /**
             * Initialize.
             *
             * @param autosync sync after each message or after multiple messages
             * @param path to the queue
             */
            void init(bool autosync, boost::filesystem::path path);

            /**
             * Send message to subscribers.
             *
             * @param msg to send to all subscribers
             */
            void send(char msg);

            /**
             * Send messages to subscribers.
             *
             * @param msgs to send to all subscribers
             */
            void send_many(std::vector<char> msgs);

            ~Publisher();
        private:
            bool autosync_;
            persipubsub::queue::Queue queue_;

        }; // class Publisher
    } // namespace publisher
} // namespace persipubsub