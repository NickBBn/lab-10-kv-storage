//
// Created by niickson on 5/9/21.
//

#ifndef KVSTORAGE_DATA_PIECE_HPP
#define KVSTORAGE_DATA_PIECE_HPP

#include <utility>
#include "DBhasher.hpp"
#include "rocksdb/db.h"
class data_piece{
 public:
  data_piece(){};
  data_piece(rocksdb::ColumnFamilyHandle* _handle, std::string _key,
             std::string _value)
      :   handle(_handle)
        , key(std::move(_key))
        , value(std::move(_value))
  {}

 private:
  rocksdb::ColumnFamilyHandle* handle;
  std::string key;
  std::string value;

  friend class DBhasher;
};

#endif  // KVSTORAGE_DATA_PIECE_HPP
