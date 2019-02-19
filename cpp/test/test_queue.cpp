// Copyright (c) 2019 Parquery AG. All rights reserved.
// Created by Selim Naji (selim.naji@parquery.com and
// Marko Ristin (marko@parquery.com) on 2019-02-15

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE TestQueue
#include <boost/test/unit_test.hpp>

#include <filesystem.h>
#include <prettyprint.hpp>
#include <unique_resource.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "../src/queue.h"

namespace fs = boost::filesystem;

class TmpDir{
public:
    TmpDir(){
        path_ = fs::unique_path();
        fs::create_directories(path_);
    }

    ~TmpDir(){
        fs::remove_all(path_);
    }
    fs::path path_;
private:

}; // class TmpDir

BOOST_AUTO_TEST_CASE(test_initialize_environment){

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024, 1024, 32UL * 1024UL * 1024UL * 1024UL);

    auto rtxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbi = lmdb::dbi::open(rtxn, nullptr);

    MDB_envinfo info;
    lmdb::env_info(env, &info);

    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(info.me_last_txnid));
    BOOST_REQUIRE_EQUAL("1", boost::lexical_cast<std::string>(info.me_last_pgno));
    BOOST_REQUIRE_EQUAL("1024", boost::lexical_cast<std::string>(info.me_maxreaders));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(info.me_mapaddr));
    BOOST_REQUIRE_EQUAL("34359738368", boost::lexical_cast<std::string>(info.me_mapsize));
    BOOST_REQUIRE_EQUAL("1", boost::lexical_cast<std::string>(info.me_numreaders));

    MDB_stat stat = dbi.stat(rtxn);

    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_branch_pages));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_entries));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_depth));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_leaf_pages));
    BOOST_REQUIRE_EQUAL("0", boost::lexical_cast<std::string>(stat.ms_overflow_pages));
    BOOST_REQUIRE_EQUAL("4096", boost::lexical_cast<std::string>(stat.ms_psize));

    rtxn.abort();
}

BOOST_AUTO_TEST_CASE(test_put_to_single_subscriber) {

    TmpDir tmp_dir = TmpDir();
    const fs::path queue_dir = tmp_dir.path_;
    lmdb::env env = persipubsub::queue::initialize_environment(queue_dir, 1024,
                                                               1024,
                                                               32UL * 1024UL *
                                                               1024UL * 1024UL);
    // todo ask Adam if '.' will be a problem.
    std::string msg = "I'm a message.\n";
    std::string name = "sub";

    auto wtxn = lmdb::txn::begin(env);
    auto dbi = lmdb::dbi::open(wtxn, name.c_str(), MDB_CREATE);
    wtxn.commit();
    // The Environment is closed automatically. For testing purpose the
    // environment is here forcefully closed.
    env.close();

    persipubsub::queue::Queue queue = persipubsub::queue::Queue();
    queue.init(queue_dir);

    std::vector<std::string> subs;
    subs.push_back(name);
    queue.put(msg, subs);

    std::string result;
    queue.front(name, result);

    BOOST_REQUIRE_EQUAL(msg, result);

}
