//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/filter_policy.h"

namespace ROCKSDB_NAMESPACE {

// Per-SST-file cuckoo filter policy. See NewCuckooFilterPolicy() in
// include/rocksdb/filter_policy.h for the user-facing description.
//
// Filters built by this policy are stored under their own
// CompatibilityName(), so they are not read by built-in Bloom/Ribbon
// policies and vice versa. A DB that switches between this policy and a
// built-in one has no usable filters on existing SST files until compaction
// rewrites them.
class CuckooFilterPolicy : public FilterPolicy {
 public:
  explicit CuckooFilterPolicy(double bits_per_key);

  static const char* kClassName();
  const char* Name() const override { return kClassName(); }
  static const char* kNickName();
  const char* NickName() const override { return kNickName(); }

  // Not part of a read-compatible family with any other policy.
  const char* CompatibilityName() const override;

  // e.g. "cuckoofilter:10" or "cuckoofilter:7.5"
  std::string GetId() const override;

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override;

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override;

  // Configured bits per key, rounded to thousandths. Essentially for testing.
  int GetMillibitsPerKey() const { return millibits_per_key_; }

  // Fingerprint width derived from bits per key. Essentially for testing.
  int GetFingerprintBits() const { return fingerprint_bits_; }

 private:
  // Target average filter space per key, in thousandths of a bit. 0 means
  // "build no filter".
  int millibits_per_key_;

  // Bits per stored fingerprint, in [4, 32].
  int fingerprint_bits_;
};

}  // namespace ROCKSDB_NAMESPACE
