
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <list>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_column_families_example";
#else
std::string kDBPath = "hmmm.db";
#endif

int main() {
  // open DB


  Options options;
  options.create_if_missing = false;
  //DB* db;

  //Status s = DB::Open(options, kDBPath,  &db);
  //assert(s.ok());

  std::string value;

  //         get column familiies descriptors
  std::vector <std::string> family;
  std::vector<ColumnFamilyDescriptor> descriptors;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(),
                                      kDBPath,
                                      &family);
  assert(status.ok());

  descriptors.reserve(family.size());
  for (const std::string &familyName : family) {
    descriptors.emplace_back(familyName,
                             rocksdb::ColumnFamilyOptions());
  }

  //
  std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handlers;
  std::vector < rocksdb::ColumnFamilyHandle * > newHandles;
  rocksdb::DB *dbStrPtr;

  status =
      rocksdb::DB::Open(
          rocksdb::DBOptions(),
          kDBPath,
          descriptors,
          &newHandles,
          &dbStrPtr);
  assert(status.ok()); //if 0 -> exit


  for (rocksdb::ColumnFamilyHandle *ptr : newHandles) {
    handlers.emplace_back(ptr);
  }

  std::cout << "newHandles.size() " << newHandles.size() <<std::endl;

  std::string valuee;
  status = dbStrPtr->Get(ReadOptions(), newHandles[1], Slice("key3"), &valuee);
  assert(status.ok());
  std::cout << valuee << std::endl;


/*
  s = db->Get(ReadOptions(), handles[1], Slice("key3"), &value);
  assert(s.ok());
  std::cout << value << std::endl;

  // drop column family
  s = db->DropColumnFamily(handles[1]);
  assert(s.ok());

  // close db
  for (auto handle : handles) {
    s = db->DestroyColumnFamilyHandle(handle);
    assert(s.ok());
  }*/

  //delete db;

  return 0;
}