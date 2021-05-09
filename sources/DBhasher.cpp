//
// Created by niickson on 4/22/21.
//

#include "DBhasher.hpp"
DBhasher::DBhasher(std::string _kDBpath)
    :   kDBpath(std::move(_kDBpath))
      , src_db(nullptr)
{}

void DBhasher::perform() {
  get_descriptors();
  rocksdb::Status status =
      rocksdb::DB::Open(
          rocksdb::DBOptions(),
          kDBpath,
          descriptors,
          &handles,
          &src_db);
  assert(status.ok());
  print_db(src_database);



}

void DBhasher::print_db(database db) {
  rocksdb::DB* cur_db;
  if (db == new_database) cur_db = new_db;
  else cur_db = src_db;
  for (auto handle : handles){
    std::cout << "Column : " << handle->GetName();
    std::cout << std::endl;
    std::unique_ptr<rocksdb::Iterator> it(
        cur_db->NewIterator(rocksdb::ReadOptions(), handle));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::cout << it->key().ToString() << " : " << it->value().ToString() << std::endl;
    }
    std::cout << std::endl;
  }
}

void DBhasher::get_descriptors() {
  std::vector <std::string> families;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(),
                                      kDBpath,
                                      &families);
  assert(status.ok());

  descriptors.reserve(families.size());
  for (const std::string &family : families) {
    descriptors.emplace_back(family,rocksdb::ColumnFamilyOptions());
  }
}
