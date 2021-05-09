//
// Created by niickson on 4/22/21.
//

#include "DBhasher.hpp"
DBhasher::DBhasher(std::string _kDBpath, std::string _new_path, size_t _threads_count)
    :   threads_count(_threads_count)
      , kDBpath(std::move(_kDBpath))
      , new_path(std::move(_new_path))
      , src_db(nullptr)
      , new_db(nullptr)
      , stop_hash(false)
      , stop_read(false)
      , stop_write(false)
      , pieces_to_hash(0)
      , pieces_to_write(0)
{
  data_to_hash = new safe_queue<data_piece>;
  data_to_write = new safe_queue<data_piece>;
}

void DBhasher::perform() {
  get_descriptors();
  rocksdb::Status status;
  status =
      rocksdb::DB::Open( rocksdb::DBOptions(), kDBpath, descriptors,
          &src_handles, &src_db);
  assert(status.ok());
  print_db(src_database);
  start_reading();
  start_hashing();
  create_new_db();
  status = rocksdb::DB::Open(rocksdb::DBOptions(),new_path,
                             descriptors, &new_handles, &new_db);
  assert(status.ok());
  start_writing();
  global_work.lock();
  print_db(new_database);
  global_work.unlock();
  close_both_db();
}

void DBhasher::print_db(database db) {
  rocksdb::DB* cur_db;
  std::vector<rocksdb::ColumnFamilyHandle*> cur_handles;
  if (db == new_database) {
    cur_db = new_db;
    cur_handles = new_handles;
  }
  else {
    cur_db = src_db;
    cur_handles = src_handles;
  }
  for (auto handle : cur_handles){
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

void DBhasher::create_new_db() {
  rocksdb::Status status;
  rocksdb::Options options;
  options.create_if_missing = true;
  status = rocksdb::DB::Open( options, new_path, &new_db);
  assert(status.ok());

  rocksdb::ColumnFamilyHandle* cf;
  for (const auto &descriptor : descriptors){
    status = new_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), descriptor.name, &cf);
    std::cout << descriptor.name << " in cycle" << std::endl;
    delete cf;
  }
  assert(status.ok());
  delete new_db;
}

void DBhasher::start_reading() {
  static const auto reading_func = [this](rocksdb::ColumnFamilyHandle* handle){
    static size_t works_counter = 0;
    ++works_counter;
    std::unique_ptr<rocksdb::Iterator> it(
        src_db->NewIterator(rocksdb::ReadOptions(), handle));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      data_to_hash->push(data_piece
                         { handle,
                          it->key().ToString(),
                          it->value().ToString()});
      ++pieces_to_hash;
      //std::cout << it->key().ToString() << " " << it->value().ToString() << " ; handle : " << handle << std::endl;
    }
    --works_counter;
    if ((works_counter == 0) && (stop_read)) stop_hash = true;
  };
  ThreadPool pool_read(threads_count);
  for (auto& handle : src_handles){
    pool_read.enqueue(reading_func, handle);
  }
  stop_read = true;
}

void DBhasher::start_hashing() {
  static const auto hashing_func = [this]{
    while (!(stop_hash && (pieces_to_hash <= 0))){
      if (!data_to_hash->is_empty()){
        data_piece data = data_to_hash->front();
        data_to_hash->pop();
        data.value = picosha2::hash256_hex_string(data.key + data.value);
        std::cout << data.key << " " << data.value << " " << data.handle << std::endl;
        data_to_write->push(std::move(data));
        --pieces_to_hash;
        ++pieces_to_write;
      }
    }
    stop_write = true;
  };
  ThreadPool pool_hash(threads_count);
  for(size_t i = 0; i < threads_count; ++i){
    pool_hash.enqueue(hashing_func);
  }
}

void DBhasher::start_writing() {
  static const auto writing_func = [this]{
    global_work.lock_shared();
    rocksdb::WriteBatch batch;
    while (!(stop_write && (pieces_to_write <= 0))){
      if (!data_to_write->is_empty()){
        data_piece data = data_to_write->front();
        data_to_write->pop();
        batch.Put(data.handle, rocksdb::Slice(data.key), rocksdb::Slice(data.value));
        //std::cout << "writing" << std::endl;
        --pieces_to_write;
      }
    }
    rocksdb::Status status = new_db->Write(rocksdb::WriteOptions(), &batch);
    assert(status.ok());
    global_work.unlock_shared();
  };
  ThreadPool pool_write(threads_count);
  for(size_t i = 0; i < threads_count; ++i){
    pool_write.enqueue(writing_func);
  }
}

void DBhasher::close_both_db() {
  rocksdb::Status status;
  for (auto handle : src_handles) {
    status = src_db->DestroyColumnFamilyHandle(handle);
    assert(status.ok());
  }
  delete src_db;
  for (auto handle : new_handles) {
    status = new_db->DestroyColumnFamilyHandle(handle);
    assert(status.ok());
  }
  delete new_db;
}

DBhasher::~DBhasher() {
  delete data_to_hash;
  delete data_to_write;
}
