// Copyright (c) 2016 Parquery AG. All rights reserved.
// Created by mristin on 10/12/16.

#pragma once

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <string>
#include <tuple>
#include <vector>

#include <cstdlib>
#include <fcntl.h>

namespace pqry {
namespace filesystem {

/**
 * Simulates Unix ls -lt
 * @param directory to list
 * @return absolute paths sorted by modification time
 */
std::vector<boost::filesystem::path> ls_lt(const boost::filesystem::path& directory);

/**
 * @param directory to be listed
 * @return list of absolute paths to files contained in the directory
 */
std::vector<boost::filesystem::path> listdir(const boost::filesystem::path& directory);

/**
 * @param path to the file
 * @return modified time in nanoseconds since epoch
 */
int64_t modified_time(const boost::filesystem::path& path);

/**
 * simulates `mkdtemp` command
 * @return path to the temporary directory
 */
boost::filesystem::path mkdtemp();

/**
 * Reads a file to the string
 * @param path to the file
 * @return content of the file
 */
std::string read(const boost::filesystem::path& path);

/**
 * Writes the whole text to the file
 * @param path to the file
 * @param text to be written
 */
void write(const boost::filesystem::path& path, const std::string& text);

/**
 * Writes the `bytes` to the file
 * @param path to write to
 * @param bytes that are to be written
 * @param size of the `bytes`
 */
void write(const boost::filesystem::path& path, const char* bytes, int size);

/**
 * Copies a source directory with all contents into a new target directory recursively
 * @param source directory that gets copied
 * @param dest directory where source gets copied to. Will be created if non-existent
 */
void copy_directory(const boost::filesystem::path& source, const boost::filesystem::path& dest);

/**
 * Unzips an archive.
 * @param path to the archive
 * @param dest_dir destination where the archive will be unzipped. Expects it to exist
 */
void unzip_archive(const boost::filesystem::path& path, const boost::filesystem::path& dest_dir);

/**
 * Uses POSIX flock() for named mutexes. Use an instance of this class to prevent multiple processes running at the
 * same time.
 *
 * Mind that the instance keeps a file descriptor open during its lifetime.
 * The implementation is crash-safe, i.e. if the program crashes, the system automatically releases the lock.
 * Unlike boost::interprocess::file_lock, we do not have to create the file manually first.
 * boost::interprocess:named_mutex did not work on Linux with boost 1.62 (marko tested it manually).
 */
class LockFileGuard {
 public:
  LockFileGuard() : locked_(false) {}

  /**
   * Creates the lock file and writes the given PID to it
   * @param lock_file to use for locking
   * @param pid of the process
   * @return true if the file could be successfully locked.
   */
  bool lock(const boost::filesystem::path& lock_file, int pid);

  void unlock();

  virtual ~LockFileGuard();

 private:
  bool locked_;
  boost::filesystem::path lock_file_;
  int fid_;
};

/**
 * Wait for file to exist. Panics if timeout exceeded (0 means no timeout).
 * @param path to the file
 * @param timeout in seconds
 */
void wait_for_file(const boost::filesystem::path& path, unsigned int timeout);

/**
 * Expands path using wordexp
 * @param path to be expanded
 * @return expanded path
 */
boost::filesystem::path expand_path(const boost::filesystem::path& path);

/**
 * Temporary scope file, created from a path with a random suffix and ".tmp".
 * If the file exists at the destruction, it will be deleted.
 * All errors will be ignored in the destructor.
 */
class NamedTempfile {
 public:
  explicit NamedTempfile(const boost::filesystem::path& path);

  /**
   * Deletes the temporary file if it was not renamed before.
   */
  ~NamedTempfile();

  const boost::filesystem::path& path() const { return tmp_pth_; }

  /**
   * Renames the temporary file to the path passed in at the constructor.
   */
  void rename();

 private:
  const boost::filesystem::path pth_;
  const boost::filesystem::path tmp_pth_;
  bool renamed_;
};

}  // namespace filesystem
}  // namespace pqry
