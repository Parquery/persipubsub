// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com/marko@parquery.com)
// on 15.02.2019

#pragma once

#include <boost/filesystem/path.hpp>
#include <vector>
#include "queue.h"
#include "control.h"
#include "publisher.h"
#include "subscriber.h"

namespace persipubsub{
    namespace environment{

        /**
         * Fabricate persipubsub components.
         */
        class Environment{

        public:
            /**
             * Initialize.
             *
             * @param path to the queue
             */
            Environment(const boost::filesystem::path& path) : path_(path) {};

            /**
             * Fabricate a new control.
             *
             * @param subscriber_ids subscribers of the queue
             * @param max_readers max number of reader of the lmdb
             * @param max_size of the lmdb in bytes
             * @param high_watermark  limit of the queue
             * @param strategy used to prune queue
             * @return Control to create and maintain queue
             */
            persipubsub::control::Control new_control(
                    std::vector<char > subscriber_ids, int max_readers,
                    int max_size,
                    persipubsub::queue::HighWaterMark high_watermark,
                    persipubsub::queue::Strategy strategy);

            /**
             * Fabricate a new publisher.
             *
             * @param autosync if True, store data automatically in lmdb
             * @return Publisher to send messages
             */
            persipubsub::publisher::Publisher new_publisher(bool autosync);

            /**
             * Fabricate a new subscriber.
             *
             * @param identifier of the subscriber
             * @return Subscriber to receive messages
             */
            persipubsub::subscriber::Subscriber new_subscriber(char identifier);

        private:
            boost::filesystem::path path_;

        }; // class Environment

        /**
         * Fabricate a new environment.
         *
         * @param path to the queue
         * @return Environment to create control, publisher and subscriber
         */
        persipubsub::environment::Environment new_environment(boost::filesystem::path path);

    } // namespace environment
} // namespace persipubsub
