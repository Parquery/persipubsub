// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

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