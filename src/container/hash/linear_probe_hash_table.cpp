//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  page_id_t header_page_id = INVALID_PAGE_ID;
  Page *header_page;
  while (header_page_id == INVALID_PAGE_ID) {
    header_page = buffer_pool_manager_->NewPage(&header_page_id, nullptr);
  }
  header_page_id_ = header_page_id;
  header_page->WLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  header->SetSize(num_buckets);
  header->SetPageId(header_page_id);
  while (header->NumBlocks() < header->GetSize()) {
    page_id_t tmp = INVALID_PAGE_ID;
    buffer_pool_manager_->NewPage(&tmp);
    if (tmp != INVALID_PAGE_ID) {
      header->AddBlockPageId(tmp);
      buffer_pool_manager_->UnpinPage(tmp, false);
    }
  }
  header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  size_t hash_val = hash_fn_.GetHash(key) % (header->NumBlocks() * BLOCK_ARRAY_SIZE);
  size_t block_ind = hash_val / BLOCK_ARRAY_SIZE;
  size_t offset = hash_val % BLOCK_ARRAY_SIZE;

  auto bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
  bucket_page->RLatch();
  auto bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
  while (bucket->IsOccupied(offset)) {
    if (bucket->IsReadable(offset) && comparator_(bucket->KeyAt(offset), key) == 0) {
      result->emplace_back(bucket->ValueAt(offset));
    }
    offset++;
    if (block_ind * BLOCK_ARRAY_SIZE + offset == hash_val) {
      // one circle
      break;
    }

    if (offset >= BLOCK_ARRAY_SIZE) {
      // next bucket
      offset = 0;
      bucket_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
      block_ind = (block_ind + 1) % header->NumBlocks();
      bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
      bucket_page->RLatch();
      bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
    }
  }

  bucket_page->RUnlatch();
  header_page->RUnlatch();
  table_latch_.RUnlock();
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  bool full = false;

  while (!InternalInsert(key, value, &full)) {
    table_latch_.RUnlock();
    if (full) {
      Resize(GetSize());
      table_latch_.RLock();
    } else {
      return false;
    }
  }
  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  size_t hash_val = hash_fn_.GetHash(key) % (header->NumBlocks() * BLOCK_ARRAY_SIZE);
  size_t block_ind = hash_val / BLOCK_ARRAY_SIZE;
  size_t offset = hash_val % BLOCK_ARRAY_SIZE;

  auto bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
  bucket_page->WLatch();
  auto bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());

  while (bucket->IsOccupied(offset)) {
    if (comparator_(bucket->KeyAt(offset), key) == 0 && bucket->ValueAt(offset) == value) {
      if (!bucket->IsReadable(offset)) {
        bucket_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
        header_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        table_latch_.RUnlock();
        return false;
      }
      bucket->Remove(offset);
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), true);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      table_latch_.RUnlock();
      return true;
    }
    offset++;
    if (block_ind * BLOCK_ARRAY_SIZE + offset == hash_val) {
      // one circle
      break;
    }

    if (offset >= BLOCK_ARRAY_SIZE) {
      // next bucket
      offset = 0;
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
      block_ind = (block_ind + 1) % header->NumBlocks();
      bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
      bucket_page->WLatch();
      bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
    }
  }
  bucket_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();

  // fetch original header
  page_id_t original_header_page_id = header_page_id_;
  auto original_header_page = buffer_pool_manager_->FetchPage(original_header_page_id);
  original_header_page->RLatch();
  auto original_header = reinterpret_cast<HashTableHeaderPage *>(original_header_page->GetData());

  // alloc new header and buckets
  page_id_t new_header_page_id = INVALID_PAGE_ID;
  Page *new_header_page;
  while (new_header_page_id == INVALID_PAGE_ID) {
    new_header_page = buffer_pool_manager_->NewPage(&new_header_page_id);
  }
  header_page_id_ = new_header_page_id;
  new_header_page->WLatch();
  auto *new_header = reinterpret_cast<HashTableHeaderPage *>(new_header_page->GetData());
  new_header->SetSize(2 * initial_size / BLOCK_ARRAY_SIZE + ((2 * initial_size) % BLOCK_ARRAY_SIZE > 0));
  new_header->SetPageId(new_header_page_id);
  new_header_page->WUnlatch();
  new_header_page->RLatch();
  while (new_header->NumBlocks() < new_header->GetSize()) {
    page_id_t tmp = INVALID_PAGE_ID;
    buffer_pool_manager_->NewPage(&tmp);
    if (tmp != INVALID_PAGE_ID) {
      new_header->AddBlockPageId(tmp);
      buffer_pool_manager_->UnpinPage(tmp, false);
    }
  }

  // mv kvs from original hash table
  for (size_t block_ind = 0; block_ind < original_header->NumBlocks(); block_ind++) {
    page_id_t bucket_page_id = original_header->GetBlockPageId(block_ind);
    auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
    bucket_page->RLatch();
    auto bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
    for (size_t offset = 0; offset < BLOCK_ARRAY_SIZE; offset++) {
      if (bucket->IsReadable(offset)) {
        auto key = bucket->KeyAt(offset);
        auto value = bucket->ValueAt(offset);
        InternalInsert(key, value);
      }
    }
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);
  }
  new_header_page->RUnlatch();
  original_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(original_header_page_id, false);
  buffer_pool_manager_->DeletePage(original_header_page_id);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  auto header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t bucket_num = header->NumBlocks();
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return bucket_num * BLOCK_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InternalInsert(const KeyType &key, const ValueType &value, bool *full) {
  auto header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  size_t hash_val = hash_fn_.GetHash(key) % (header->NumBlocks() * BLOCK_ARRAY_SIZE);
  size_t block_ind = hash_val / BLOCK_ARRAY_SIZE;
  size_t offset = hash_val % BLOCK_ARRAY_SIZE;
  auto bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
  bucket_page->WLatch();
  auto bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
  while (!bucket->Insert(offset, key, value)) {
    if (comparator_(bucket->KeyAt(offset), key) == 0 && bucket->ValueAt(offset) == value) {
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return false;
    }
    offset++;
    if (block_ind * BLOCK_ARRAY_SIZE + offset == hash_val) {
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      if (full != nullptr) {
        *full = true;
      }
      return false;
    }
    if (offset >= BLOCK_ARRAY_SIZE) {
      // next bucket
      offset = 0;
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), false);
      block_ind = (block_ind + 1) % header->NumBlocks();
      bucket_page = buffer_pool_manager_->FetchPage(header->GetBlockPageId(block_ind));
      bucket_page->WLatch();
      bucket = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(bucket_page->GetData());
    }
  }
  bucket_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header->GetBlockPageId(block_ind), true);
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return true;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
