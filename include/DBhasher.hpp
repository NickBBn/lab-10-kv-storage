//
// Created by niickson on 4/22/21.
//

#ifndef INCLUDE_DBHASHER_HPP_
#define INCLUDE_DBHASHER_HPP_

#include <iostream>
#include <rocksdb/db.h>
enum database{
  new_database [[maybe_unused]] = 0,
  src_database [[maybe_unused]] = 1
};

class DBhasher {
 public:
  explicit DBhasher(std::string _kDBpath);
  void perform();
  void print_db(database db);
  void get_descriptors();

 private:
  std::string kDBpath;
  rocksdb::DB* src_db;
  rocksdb::DB* new_db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
};

#endif  // INCLUDE_DBHASHER_HPP_
