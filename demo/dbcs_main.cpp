
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
#include "DBhasher.hpp"

using namespace rocksdb;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_column_families_example";
#else
std::string kDBPath = "hmmm.db";
#endif

int main() {

  DBhasher hasher(kDBPath, "new_db", 3);
  hasher.perform();

  return 0;
}