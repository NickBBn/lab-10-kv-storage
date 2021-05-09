//
// Created by niickson on 4/22/21.
//

#ifndef INCLUDE_DBHASHER_HPP_
#define INCLUDE_DBHASHER_HPP_

#include "PicoSHA2/picosha2.h"
#include "ThreadPool/ThreadPool.h"
#include <iostream>
#include "safe_queue.hpp"
#include "data_piece.hpp"
#include <rocksdb/db.h>
#include "atomic"
#include "rocksdb/slice.h"
#include "shared_mutex"

enum database{
  new_database [[maybe_unused]] = 0,
  src_database [[maybe_unused]] = 1
};

class DBhasher {
 public:
  explicit DBhasher(std::string _kDBpath, std::string _new_path,
                    size_t _threads_count);
  ~DBhasher();
  void perform();
  void print_db(database db);
  void get_descriptors();
  void create_new_db();
  void start_reading();
  void start_hashing();
  void start_writing();
  void close_both_db();

 private:
  size_t threads_count;
  std::string kDBpath;
  std::string new_path;
  rocksdb::DB* src_db;
  rocksdb::DB* new_db;
  std::vector<rocksdb::ColumnFamilyHandle*> src_handles;
  std::vector<rocksdb::ColumnFamilyHandle*> new_handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  safe_queue<data_piece>* data_to_hash;
  safe_queue<data_piece>* data_to_write;
  std::atomic_bool stop_hash;
  std::atomic_bool stop_read;
  std::atomic_bool stop_write;
  std::atomic_int pieces_to_hash;
  std::atomic_int pieces_to_write;
  std::shared_mutex global_work;
};

#endif  // INCLUDE_DBHASHER_HPP_
