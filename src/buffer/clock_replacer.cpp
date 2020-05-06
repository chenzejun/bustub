//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : clock_hand_(0), clock_size_(0) {
  Meta meta = {true, false};
  for (size_t i = 0; i < num_pages; i++) {
    frames_.emplace_back(meta);
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);
  while (clock_size_ > 0) {
    clock_hand_ %= frames_.size();
    if (frames_[clock_hand_].pin_) {
      clock_hand_++;
    } else if (frames_[clock_hand_].ref_) {
      frames_[clock_hand_].ref_ = false;
      clock_hand_++;
    } else {
      frames_[clock_hand_].pin_ = true;
      clock_size_--;
      *frame_id = clock_hand_++;
      return true;
    }
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);
  if (static_cast<size_t>(frame_id) >= frames_.size()) {
    return;
  }
  if (!frames_[frame_id].pin_) {
    frames_[frame_id].pin_ = true;
    clock_size_--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);
  if (static_cast<size_t>(frame_id) >= frames_.size()) {
    return;
  }
  if (frames_[frame_id].pin_) {
    clock_size_++;
  }
  frames_[frame_id].pin_ = false;
  frames_[frame_id].ref_ = true;
}

size_t ClockReplacer::Size() {
  std::scoped_lock<std::mutex> lock(mutex_);
  return clock_size_;
}

}  // namespace bustub
