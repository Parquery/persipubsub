// Copyright (c) 2016 Parquery AG. All rights reserved.
// Created by mristin on 10/12/16.

#include "filesystem.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <boost/range/iterator_range.hpp>

#include <unique_resource.hpp>
#include <tinyformat.h>

#include <algorithm>
#include <fstream>
#include <list>
#include <map>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <wordexp.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace fs = boost::filesystem;

std::vector<fs::path> pqry::filesystem::ls_lt(const fs::path& directory) {
  if (!fs::exists(directory)) {
    throw std::invalid_argument(tfm::format("Directory must exist: %s", directory));
  }

  if (!fs::is_directory(directory)) {
    throw std::invalid_argument(
        tfm::format("Not a directory: %s %s", directory, fs::status(directory).type()));
  }

  std::list<std::tuple<int64_t, fs::path> > ts_pths;
  for (const auto& entry : boost::make_iterator_range(fs::directory_iterator(directory), {})) {
    struct stat st = {};
    stat(entry.path().c_str(), &st);

    // get the time in nanoseconds
    const int64_t ns = pqry::filesystem::modified_time(entry.path());

    ts_pths.emplace_back(std::make_tuple(ns, entry.path()));
  }

  ts_pths.sort();

  std::vector<fs::path> res;
  res.reserve(ts_pths.size());

  for (const auto& ts_pth : ts_pths) {
    res.push_back(std::get<1>(ts_pth));
  }

  return res;
}

std::vector<fs::path> pqry::filesystem::listdir(const fs::path& directory) {
  if (not fs::exists(directory)) {
    std::printf( "Directory does not exist");
  }

  if (not fs::is_directory(directory)) {
    std::printf( "Not a directory");
  }

  std::vector<fs::path> res;
  for (const auto& entry : boost::make_iterator_range(fs::directory_iterator(directory), {})) {
    res.push_back(entry.path());
  }
  return res;
}

int64_t pqry::filesystem::modified_time(const fs::path& path) {
  struct stat st = {};
  stat(path.c_str(), &st);

  // get the time in nanoseconds
  const int64_t ns(
      static_cast<int64_t>(st.st_mtim.tv_sec * 1000L * 1000L * 1000L) + static_cast<int64_t>(st.st_mtim.tv_nsec));

  return ns;
}

fs::path pqry::filesystem::mkdtemp() {
  const fs::path tmpdir = fs::temp_directory_path() / fs::unique_path();
  fs::create_directories(tmpdir);

  return tmpdir;
}

std::string pqry::filesystem::read(const fs::path& path) {
  std::string content;

  std::ifstream ifs(path.c_str(), std::ios::in | std::ios::binary | std::ios::ate);

  if (ifs.good()) {
    auto fsize = ifs.tellg();
    if (fsize < 0) {
      std::printf( "File size retrieval failed");
    }
    auto fsize_nonneg = static_cast<size_t>(fsize);

    ifs.seekg(0, std::ios::beg);

    std::vector<char> bytes(fsize_nonneg);
    ifs.read(&bytes[0], fsize);

    if (ifs.good()) {
      content = std::string(&bytes[0], fsize_nonneg);
    } else {
      std::printf( "error reading");
    }
  } else {
    std::printf( "error reading");
  }

  return content;
}

void pqry::filesystem::write(const fs::path& path, const std::string& text) {
  std::ofstream ofs(path.c_str(), std::ios::out | std::ios::binary);
  ofs << text;

  if (!ofs.good()) {
    std::printf( "error writing");
  }
}

void pqry::filesystem::write(const fs::path& path, const char* bytes, int size) {
  std::ofstream ofs(path.c_str(), std::ios::out | std::ios::binary);

  if (!ofs.good()) {
    std::printf( "error writing");
  }

  ofs.write(bytes, size);
}

void pqry::filesystem::copy_directory(const fs::path& source, const fs::path& dest) {
  if (not fs::exists(source)) {
    std::printf( "Error attempting to copy: Source does not exist");
  }
  if (not fs::is_directory(source)) {
    std::printf( "Error attempting to copy: Source is not a directory");
  }
  if (not fs::exists(dest)) {
    if (not fs::create_directories(dest)) {
      std::printf( "Error attempting to create destination folder");
    }
  }

  using rec_iterator = fs::recursive_directory_iterator;
  for (auto it = rec_iterator(source, fs::symlink_option::no_recurse), end = rec_iterator(); it != end; ++it) {
    // Remove common root
    std::string difference = it->path().string();
    boost::replace_first(difference, source.string(), "");

    if (not fs::exists(dest / difference)) {
      boost::system::error_code error;
      fs::copy(it->path(), dest / difference, error);
      if (error.value() != 0) {
        std::printf("Failed to copy");
      }
    } else {
      if (not fs::is_directory(it->path())) {
        fs::copy_file(it->path(), dest / difference, fs::copy_option::overwrite_if_exists);
      }
    }
  }
}

void pqry::filesystem::unzip_archive(const fs::path& path, const fs::path& dest_dir) {
  if (not fs::exists(path)) {
    std::printf( "File does not exist");
  }
  if (not fs::exists(dest_dir)) {
    std::printf( "Expected the destination directory to exist");
  }
  if (not fs::is_directory(dest_dir)) {
    std::printf( "Not a directory");
  }

  // Read from the archive
  //  libzippp::ZipArchive zf(path.c_str());
  //  zf.open(libzippp::ZipArchive::READ_ONLY);
  //
  //  std::vector<libzippp::ZipEntry> entries = zf.getEntries();
  //  std::vector<libzippp::ZipEntry>::iterator it;
  //  for (it = entries.begin(); it != entries.end(); ++it) {
  //    libzippp::ZipEntry entry = *it;
  //
  //    // Create directory if entry is a dir
  //    if (entry.isDirectory()) {
  //      fs::path dir(dest_dir / entry.getName());
  //      if (not fs::is_directory(dir.parent_path())) {
  //        fs::create_directory(dir.parent_path());
  //      }
  //      continue;
  //    }
  //
  //    // Only process if entry is a file
  //    if (not entry.isFile()) {
  //      continue;
  //    }
  //
  //    fs::path entry_path(dest_dir / entry.getName());
  //    std::ofstream ofs(entry_path.string());
  //    entry.readContent(ofs);
  //    ofs.close();
  // }
}

bool pqry::filesystem::LockFileGuard::lock(const fs::path& lock_file, int pid) {
  if (locked_) {
    unlock();
  }

  if (lock_file.empty()) {
    std::printf(R"(lock_file path is "")");
  }

  fid_ = ::open(lock_file.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fid_ == -1) {
    std::printf( "error opening the lock file for writing");
  }

  const int rc = ::flock(fid_, LOCK_EX | LOCK_NB);

  if (fid_ >= 0 and rc < 0) {
    ::close(fid_);
    locked_ = false;
    return locked_;
  }

  const std::string pid_str = tfm::format("%d", pid);
  const ssize_t n = ::write(fid_, pid_str.c_str(), pid_str.size());
  if (n != pid_str.size()) {
    std::printf( "error writing to lock file");
  }

  lock_file_ = lock_file;
  locked_ = true;
  return locked_;
}

void pqry::filesystem::LockFileGuard::unlock() {
  if (locked_) {
    std::printf("Deleting lock file");
    ::close(fid_);
    fs::remove(lock_file_);
  }
}

pqry::filesystem::LockFileGuard::~LockFileGuard() {
  unlock();
}

void pqry::filesystem::wait_for_file(const fs::path& path, unsigned int timeout) {
  int i = 0;
  while (not fs::exists(path)) {
    ::sleep(1);
    ++i;
    if (timeout > 0 and i > timeout) {
      std::printf( "File does not exist (waited for some seconds)");
    }
  }
}

fs::path pqry::filesystem::expand_path(const fs::path& path) {
  wordexp_t p = {};
  const int ret = wordexp(path.c_str(), &p, 0);
  if (ret != 0) {
    wordfree(&p);
    std::printf( "Wordexp failed: %d", ret);
  }

  fs::path expanded(p.we_wordv[0]);  // NOLINT
  wordfree(&p);

  return expanded;
}

/**
 * @param path to be added a suffix for a named temporary file
 * @return path with a unique suffix
 */
fs::path with_temp_suffix(const fs::path& path) {
  return path.parent_path() /
      (path.stem().string() + "." + boost::filesystem::unique_path().string() + ".tmp" + path.extension().string());
}

pqry::filesystem::NamedTempfile::NamedTempfile(
    const boost::filesystem::path& path) : pth_(path), tmp_pth_(with_temp_suffix(path)), renamed_(false) {}

pqry::filesystem::NamedTempfile::~NamedTempfile() {
  if (not renamed_) {
    boost::system::error_code ec;  // ignore all errors
    fs::remove(tmp_pth_, ec);
  }
}

void pqry::filesystem::NamedTempfile::rename() {
  if (renamed_) {
    std::printf( "The temporary file has been already renamed");
  }
  fs::rename(tmp_pth_, pth_);
  renamed_ = true;
}
