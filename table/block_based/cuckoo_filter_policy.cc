//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Per-SST-file cuckoo filter, following Fan, Andersen, Kaminsky and
// Mitzenmacher, "Cuckoo Filter: Practically Better Than Bloom" (CoNEXT 2014).
//
// Each key hash maps to a fingerprint fp and two candidate buckets b1, b2 of
// kSlotsPerBucket slots each. A key may be present iff fp is stored in b1 or
// b2. Unlike a dynamic cuckoo filter, the whole key set is known when the
// filter is built (FilterBitsBuilder::Finish), so the table is sized for that
// set and filled in one pass.
//
// Filter data layout:
//
//   0 +--------------------------------------------------+
//     | num_buckets * kSlotsPerBucket fingerprints,       |
//     | fingerprint_bits each, densely bit-packed,        |
//     | little-endian bit order                           |
//   A +--------------------------------------------------+  A = ArrayBytes()
//     | fixed32 num_buckets                               |
//     | uint8   fingerprint_bits                          |
//     | uint8   slots per bucket (kSlotsPerBucket)        |
//     | uint8   format version (kFormatVersion)           |
//     | uint8   magic (kMagic)                            |
//   A + kMetadataLen +-------------------------------------+
//
// A zero-length filter means "no keys" (always false). A well-formed trailer
// with num_buckets == 0 means "always true" (construction failed). Anything
// unrecognized is also treated as always true, which is safe.

#include "table/block_based/cuckoo_filter_policy.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/multiget_context.h"
#include "util/atomic.h"
#include "util/coding.h"
#include "util/fastrange.h"
#include "util/hash.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// ######################## Schema-critical constants ##################### //
// Changing any of these (or CuckooHasher) changes the meaning of existing
// filter data; bump kFormatVersion if that is ever intended.

constexpr uint32_t kSlotsPerBucket = 4;
constexpr int kMinFingerprintBits = 4;
constexpr int kMaxFingerprintBits = 32;
constexpr size_t kMetadataLen = 8;
constexpr uint8_t kFormatVersion = 1;
constexpr uint8_t kMagic = 0xC7;
// Golden-ratio multiplier for hashing a fingerprint to a bucket offset.
constexpr uint32_t kFingerprintMultiplier = 0x9E3779B1U;

// The reader loads 8 bytes starting at the byte holding a slot's first bit.
// That load may run past the fingerprint array by up to 7 bytes, which must
// land inside the trailer.
static_assert(kMetadataLen >= 7, "trailer must cover over-reads");

// ######################## Construction tuning ########################## //
// These affect only how filters are built, not how they are read.

// Highest load factor (fraction of occupied slots) the table is sized for.
// Kept as a rational number so sizing is exact integer arithmetic.
constexpr uint64_t kMaxLoadNumerator = 95;
constexpr uint64_t kMaxLoadDenominator = 100;
// Evictions to attempt before giving up on inserting one key.
constexpr uint32_t kMaxKicks = 500;
// Each failed attempt grows the table by ~1/16 and starts over.
constexpr int kMaxBuildAttempts = 8;
// Largest fingerprint array we will produce (keeps filter length in 32 bits).
constexpr uint64_t kMaxArrayBytes = uint64_t{0xffffffff} - kMetadataLen;

// Bytes needed to hold all fingerprints of a table.
uint64_t ArrayBytes(uint64_t num_buckets, int fingerprint_bits) {
  return (num_buckets * kSlotsPerBucket * uint64_t(fingerprint_bits) + 7) / 8;
}

// Derives fingerprint and candidate buckets from a 64-bit key hash.
// Schema-critical.
class CuckooHasher {
 public:
  CuckooHasher(uint32_t num_buckets, int fingerprint_bits)
      : num_buckets_(num_buckets), fingerprint_bits_(fingerprint_bits) {
    assert(num_buckets_ > 0);
    assert(fingerprint_bits_ >= kMinFingerprintBits &&
           fingerprint_bits_ <= kMaxFingerprintBits);
  }

  uint32_t Bucket1(uint64_t h) const {
    return FastRange32(Upper32of64(h), num_buckets_);
  }

  // Top fingerprint_bits_ of the lower 32 hash bits, independent of
  // Bucket1. 0 is reserved for "empty slot", so it is remapped to 1.
  uint32_t Fingerprint(uint64_t h) const {
    uint32_t fp = Lower32of64(h) >> (32 - fingerprint_bits_);
    return fp == 0 ? 1 : fp;
  }

  // The other candidate bucket for fp, given one of them. Computed as
  // (H(fp) - bucket) mod num_buckets_, which is an involution
  // (AltBucket(AltBucket(b, fp), fp) == b) for ANY num_buckets_. That
  // removes the usual power-of-two table size requirement of XOR-based
  // partial-key cuckoo hashing, so space can track bits/key closely.
  uint32_t AltBucket(uint32_t bucket, uint32_t fp) const {
    uint32_t hfp = FastRange32(fp * kFingerprintMultiplier, num_buckets_);
    return hfp >= bucket ? hfp - bucket : num_buckets_ - (bucket - hfp);
  }

  uint32_t GetNumBuckets() const { return num_buckets_; }

 private:
  uint32_t num_buckets_;
  int fingerprint_bits_;
};

uint32_t FingerprintMask(int fingerprint_bits) {
  return fingerprint_bits >= 32 ? 0xffffffffU
                                : (uint32_t{1} << fingerprint_bits) - 1;
}

// Byte offset of a bucket's first fingerprint bit, for prefetching.
uint64_t BucketBitOffset(uint32_t bucket, int fingerprint_bits) {
  return uint64_t{bucket} * kSlotsPerBucket * uint64_t(fingerprint_bits);
}

// Whether fp is stored in the given bucket of a packed fingerprint array.
// Requires kMetadataLen readable bytes past the array (see static_assert).
inline bool PackedBucketContains(const char* data, uint32_t bucket, uint32_t fp,
                                 int fingerprint_bits, uint32_t mask) {
  uint64_t bit = BucketBitOffset(bucket, fingerprint_bits);
  if (fingerprint_bits * int{kSlotsPerBucket} + 7 <= 64) {
    // Whole bucket fits in one 64-bit load (fingerprint_bits <= 14)
    uint64_t word = DecodeFixed64(data + (bit >> 3)) >> (bit & 7);
    for (uint32_t j = 0; j < kSlotsPerBucket; ++j) {
      if (((word >> (j * fingerprint_bits)) & mask) == fp) {
        return true;
      }
    }
    return false;
  }
  for (uint32_t j = 0; j < kSlotsPerBucket; ++j) {
    uint64_t slot_bit = bit + j * uint64_t(fingerprint_bits);
    uint64_t word = DecodeFixed64(data + (slot_bit >> 3)) >> (slot_bit & 7);
    if ((word & mask) == fp) {
      return true;
    }
  }
  return false;
}

// ############################# Readers ################################ //

class CuckooAlwaysTrueFilter : public BuiltinFilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return true; }
  using FilterBitsReader::MayMatch;  // inherit overload
  bool HashMayMatch(const uint64_t) override { return true; }
};

class CuckooAlwaysFalseFilter : public BuiltinFilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return false; }
  using FilterBitsReader::MayMatch;  // inherit overload
  bool HashMayMatch(const uint64_t) override { return false; }
};

class CuckooFilterBitsReader : public BuiltinFilterBitsReader {
 public:
  // data must stay valid for the lifetime of the reader and include the
  // trailer.
  CuckooFilterBitsReader(const char* data, uint32_t num_buckets,
                         int fingerprint_bits)
      : data_(data),
        hasher_(num_buckets, fingerprint_bits),
        fingerprint_bits_(fingerprint_bits),
        mask_(FingerprintMask(fingerprint_bits)) {}

  // No copy allowed
  CuckooFilterBitsReader(const CuckooFilterBitsReader&) = delete;
  void operator=(const CuckooFilterBitsReader&) = delete;

  bool MayMatch(const Slice& key) override {
    return HashMayMatch(GetSliceHash64(key));
  }

  void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    assert(num_keys <= MultiGetContext::MAX_BATCH_SIZE);
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> fps;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> b1s;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> b2s;
    // First pass: compute locations and prefetch both buckets of every key,
    // so the cache misses overlap.
    for (int i = 0; i < num_keys; ++i) {
      uint64_t h = GetSliceHash64(*keys[i]);
      fps[i] = hasher_.Fingerprint(h);
      b1s[i] = hasher_.Bucket1(h);
      b2s[i] = hasher_.AltBucket(b1s[i], fps[i]);
      PREFETCH(data_ + (BucketBitOffset(b1s[i], fingerprint_bits_) >> 3), 0, 3);
      PREFETCH(data_ + (BucketBitOffset(b2s[i], fingerprint_bits_) >> 3), 0, 3);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] =
          PackedBucketContains(data_, b1s[i], fps[i], fingerprint_bits_,
                               mask_) ||
          PackedBucketContains(data_, b2s[i], fps[i], fingerprint_bits_, mask_);
    }
  }

  bool HashMayMatch(const uint64_t h) override {
    uint32_t fp = hasher_.Fingerprint(h);
    uint32_t b1 = hasher_.Bucket1(h);
    if (PackedBucketContains(data_, b1, fp, fingerprint_bits_, mask_)) {
      return true;
    }
    uint32_t b2 = hasher_.AltBucket(b1, fp);
    return PackedBucketContains(data_, b2, fp, fingerprint_bits_, mask_);
  }

 private:
  const char* data_;
  const CuckooHasher hasher_;
  const int fingerprint_bits_;
  const uint32_t mask_;
};

BuiltinFilterBitsReader* NewCuckooFilterBitsReader(const Slice& contents) {
  if (contents.size() == 0) {
    // No keys were added
    return new CuckooAlwaysFalseFilter();
  }
  if (contents.size() < kMetadataLen) {
    // Unrecognized; safe default
    return new CuckooAlwaysTrueFilter();
  }
  const char* meta = contents.data() + contents.size() - kMetadataLen;
  uint32_t num_buckets = DecodeFixed32(meta);
  int fingerprint_bits = static_cast<uint8_t>(meta[4]);
  uint8_t slots_per_bucket = static_cast<uint8_t>(meta[5]);
  uint8_t version = static_cast<uint8_t>(meta[6]);
  uint8_t magic = static_cast<uint8_t>(meta[7]);
  if (magic != kMagic || version != kFormatVersion ||
      slots_per_bucket != kSlotsPerBucket ||
      fingerprint_bits < kMinFingerprintBits ||
      fingerprint_bits > kMaxFingerprintBits || num_buckets == 0 ||
      ArrayBytes(num_buckets, fingerprint_bits) + kMetadataLen !=
          contents.size()) {
    // Unrecognized/corrupt, or the builder's deliberate always-true marker
    return new CuckooAlwaysTrueFilter();
  }
  return new CuckooFilterBitsReader(contents.data(), num_buckets,
                                    fingerprint_bits);
}

void WriteMetadata(char* meta, uint32_t num_buckets, int fingerprint_bits) {
  EncodeFixed32(meta, num_buckets);
  meta[4] = static_cast<char>(fingerprint_bits);
  meta[5] = static_cast<char>(kSlotsPerBucket);
  meta[6] = static_cast<char>(kFormatVersion);
  meta[7] = static_cast<char>(kMagic);
}

// ############################# Builder ################################ //

class CuckooFilterBitsBuilder : public FilterBitsBuilder {
 public:
  CuckooFilterBitsBuilder(int millibits_per_key, int fingerprint_bits,
                          bool detect_filter_construct_corruption)
      : millibits_per_key_(millibits_per_key),
        fingerprint_bits_(fingerprint_bits),
        detect_filter_construct_corruption_(
            detect_filter_construct_corruption) {
    assert(millibits_per_key_ > 0);
  }

  // No copy allowed
  CuckooFilterBitsBuilder(const CuckooFilterBitsBuilder&) = delete;
  void operator=(const CuckooFilterBitsBuilder&) = delete;

  void AddKey(const Slice& key) override {
    uint64_t hash = GetSliceHash64(key);
    // Keys arrive sorted, so repeats are adjacent; collapse them here.
    // (Non-adjacent repeats are handled during construction.)
    if (hashes_.empty() || hash != hashes_.back()) {
      AddHash(hash);
    }
  }

  // Same de-duplication contract as XXPH3FilterBitsBuilder::AddKeyAndAlt.
  void AddKeyAndAlt(const Slice& key, const Slice& alt) override {
    uint64_t key_hash = GetSliceHash64(key);
    uint64_t alt_hash = GetSliceHash64(alt);
    std::optional<uint64_t> prev_key_hash;
    if (!hashes_.empty()) {
      prev_key_hash = hashes_.back();
    }
    const std::optional<uint64_t> prev_alt_hash = prev_alt_hash_;
    // Add alt first, so that hashes_.back() always holds the previous key
    if (alt_hash != prev_alt_hash && alt_hash != key_hash &&
        alt_hash != prev_key_hash) {
      AddHash(alt_hash);
    }
    // Overwrite for cases like alt_hash == key_hash
    prev_alt_hash_ = alt_hash;
    // Compare against the PREVIOUS alt, e.g. for key == prefix(key) at the
    // end of a prefix group under a reverse comparator
    if (key_hash != prev_key_hash && key_hash != prev_alt_hash) {
      AddHash(key_hash);
    }
  }

  // Thread-safe; may be called from background threads during parallel
  // compression.
  size_t EstimateEntriesAdded() override { return num_entries_.LoadRelaxed(); }

  using FilterBitsBuilder::Finish;

  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    return Finish(buf, nullptr);
  }

  Slice Finish(std::unique_ptr<const char[]>* buf, Status* status) override {
    if (status) {
      *status = Status::OK();
    }
    if (hashes_.empty()) {
      return Slice(nullptr, 0);
    }

    // Build into one uint32_t per slot (simple and fast to update), then
    // bit-pack. Temporary memory is ~4 bytes per slot plus the 8-byte key
    // hashes; this is NOT charged to the block cache.
    std::vector<uint32_t> slots;
    uint64_t num_buckets = NumBucketsFor(hashes_.size());
    bool built = false;
    for (int attempt = 0; attempt < kMaxBuildAttempts; ++attempt) {
      if (ArrayBytes(num_buckets, fingerprint_bits_) > kMaxArrayBytes) {
        break;
      }
      if (TryBuild(static_cast<uint32_t>(num_buckets), &slots)) {
        built = true;
        break;
      }
      // A larger table both lowers load and reshuffles every bucket
      // assignment (FastRange32 depends on num_buckets).
      num_buckets += num_buckets / 16 + 1;
    }

    if (!built) {
      // Practically unreachable at the configured load factor. Emit a
      // filter that never filters, which is correct but useless.
      std::unique_ptr<char[]> mutable_buf(new char[kMetadataLen]());
      WriteMetadata(mutable_buf.get(), /*num_buckets=*/0, fingerprint_bits_);
      Slice rv(mutable_buf.get(), kMetadataLen);
      *buf = std::move(mutable_buf);
      ResetEntries();
      return rv;
    }

    const size_t array_bytes =
        static_cast<size_t>(ArrayBytes(num_buckets, fingerprint_bits_));
    const size_t len_with_metadata = array_bytes + kMetadataLen;
    std::unique_ptr<char[]> mutable_buf(new char[len_with_metadata]());
    char* data = mutable_buf.get();

    // Bit-pack. Each OR touches only this slot's bits, which lie inside the
    // array, so the 8-byte read-modify-write never alters trailer bits.
    for (size_t s = 0; s < slots.size(); ++s) {
      if (slots[s] == 0) {
        continue;
      }
      uint64_t bit = uint64_t{s} * uint64_t(fingerprint_bits_);
      char* p = data + (bit >> 3);
      EncodeFixed64(p, DecodeFixed64(p) | (uint64_t{slots[s]} << (bit & 7)));
    }
    WriteMetadata(data + array_bytes, static_cast<uint32_t>(num_buckets),
                  fingerprint_bits_);

    if (!detect_filter_construct_corruption_) {
      ResetEntries();
    }  // else keep hashes for MaybePostVerify

    Slice rv(data, len_with_metadata);
    *buf = std::move(mutable_buf);
    return rv;
  }

  Status MaybePostVerify(const Slice& filter_content) override {
    if (!detect_filter_construct_corruption_) {
      return Status::OK();
    }
    Status s;
    std::unique_ptr<BuiltinFilterBitsReader> reader(
        NewCuckooFilterBitsReader(filter_content));
    for (uint64_t h : hashes_) {
      if (!reader->HashMayMatch(h)) {
        s = Status::Corruption("Corrupted cuckoo filter content");
        break;
      }
    }
    ResetEntries();
    return s;
  }

  size_t ApproximateNumEntries(size_t bytes) override {
    if (bytes <= kMetadataLen) {
      return 0;
    }
    uint64_t num_buckets = (uint64_t{bytes} - kMetadataLen) * 8 /
                           (kSlotsPerBucket * uint64_t(fingerprint_bits_));
    // Invert both constraints in NumBucketsFor()
    uint64_t by_space = num_buckets * kSlotsPerBucket * 1000 *
                        uint64_t(fingerprint_bits_) /
                        uint64_t(millibits_per_key_);
    uint64_t by_load =
        num_buckets * kSlotsPerBucket * kMaxLoadNumerator / kMaxLoadDenominator;
    return static_cast<size_t>(std::min(by_space, by_load));
  }

  size_t CalculateSpace(size_t num_entries) override {
    if (num_entries == 0) {
      return 0;
    }
    return static_cast<size_t>(
        ArrayBytes(NumBucketsFor(num_entries), fingerprint_bits_) +
        kMetadataLen);
  }

  // Each query compares against up to 2 * kSlotsPerBucket stored
  // fingerprints; each slot is occupied with probability ~load and then
  // matches a random non-zero fingerprint with probability 1/(2^f - 1).
  double EstimatedFpRate(size_t num_entries, size_t bytes) override {
    if (num_entries == 0) {
      return 0.0;
    }
    if (bytes <= kMetadataLen) {
      return 1.0;
    }
    double num_slots = double((uint64_t{bytes} - kMetadataLen) * 8 /
                              uint64_t(fingerprint_bits_));
    if (num_slots < 1.0) {
      return 1.0;
    }
    double load = std::min(1.0, double(num_entries) / num_slots);
    double one_slot_fp = 1.0 / double((uint64_t{1} << fingerprint_bits_) - 1);
    return 1.0 - std::pow(1.0 - one_slot_fp, 2.0 * kSlotsPerBucket * load);
  }

 private:
  void AddHash(uint64_t hash) {
    hashes_.push_back(hash);
    num_entries_.FetchAddRelaxed(1);
  }

  void ResetEntries() {
    hashes_.clear();
    num_entries_.StoreRelaxed(0);
    prev_alt_hash_ = {};
  }

  // Smallest table meeting both the bits/key space target and the maximum
  // load factor. Exact integer arithmetic for reproducible sizing.
  uint64_t NumBucketsFor(size_t num_entries) const {
    uint64_t n = num_entries;
    uint64_t space_denominator =
        uint64_t{kSlotsPerBucket} * 1000 * uint64_t(fingerprint_bits_);
    uint64_t by_space =
        (n * uint64_t(millibits_per_key_) + space_denominator - 1) /
        space_denominator;
    uint64_t load_denominator = kSlotsPerBucket * kMaxLoadNumerator;
    uint64_t by_load =
        (n * kMaxLoadDenominator + load_denominator - 1) / load_denominator;
    return std::max({uint64_t{1}, by_space, by_load});
  }

  // Tries to place every hash in a table of num_buckets buckets. Returns
  // false if some insertion exceeds kMaxKicks evictions.
  bool TryBuild(uint32_t num_buckets, std::vector<uint32_t>* slots) const {
    slots->assign(size_t{num_buckets} * kSlotsPerBucket, 0);
    uint32_t* table = slots->data();
    CuckooHasher hasher(num_buckets, fingerprint_bits_);
    // Deterministic, so the same keys always produce the same filter
    Random rnd(num_buckets);

    auto bucket_contains = [&](uint32_t b, uint32_t fp) {
      const uint32_t* s = table + size_t{b} * kSlotsPerBucket;
      for (uint32_t j = 0; j < kSlotsPerBucket; ++j) {
        if (s[j] == fp) {
          return true;
        }
      }
      return false;
    };
    auto try_insert = [&](uint32_t b, uint32_t fp) {
      uint32_t* s = table + size_t{b} * kSlotsPerBucket;
      for (uint32_t j = 0; j < kSlotsPerBucket; ++j) {
        if (s[j] == 0) {
          s[j] = fp;
          return true;
        }
      }
      return false;
    };

    for (uint64_t h : hashes_) {
      uint32_t fp = hasher.Fingerprint(h);
      uint32_t b1 = hasher.Bucket1(h);
      uint32_t b2 = hasher.AltBucket(b1, fp);
      // If fp is already in one of this key's buckets, the key is already
      // represented. Any copy of fp in b1 or b2 can only ever be evicted to
      // the other of the two (AltBucket is an involution), so it is never
      // lost. This also absorbs non-adjacent duplicate keys, which would
      // otherwise overflow both buckets after 2 * kSlotsPerBucket copies.
      if (bucket_contains(b1, fp) || bucket_contains(b2, fp)) {
        continue;
      }
      if (try_insert(b1, fp) || try_insert(b2, fp)) {
        continue;
      }
      // Random-walk eviction
      uint32_t b = rnd.OneIn(2) ? b1 : b2;
      bool placed = false;
      for (uint32_t kick = 0; kick < kMaxKicks; ++kick) {
        uint32_t j = rnd.Uniform(int{kSlotsPerBucket});
        std::swap(fp, table[size_t{b} * kSlotsPerBucket + j]);
        b = hasher.AltBucket(b, fp);
        if (try_insert(b, fp)) {
          placed = true;
          break;
        }
      }
      if (!placed) {
        return false;
      }
    }
    return true;
  }

  const int millibits_per_key_;
  const int fingerprint_bits_;
  const bool detect_filter_construct_corruption_;

  // Key hashes in insertion order. A deque avoids copying on growth.
  std::deque<uint64_t> hashes_;
  RelaxedAtomic<size_t> num_entries_{0};
  // Single-element cache for AddKeyAndAlt de-duplication
  std::optional<uint64_t> prev_alt_hash_;
};

}  // namespace

// ############################# Policy ################################# //

CuckooFilterPolicy::CuckooFilterPolicy(double bits_per_key) {
  // Sanitize like BloomLikeFilterPolicy: <= 0 (or NaN) means no filter,
  // and cap at a value well beyond any useful setting.
  if (!(bits_per_key > 0.0)) {
    millibits_per_key_ = 0;
  } else if (bits_per_key > 100.0) {
    millibits_per_key_ = 100000;
  } else {
    millibits_per_key_ = static_cast<int>(bits_per_key * 1000.0 + 0.500001);
    millibits_per_key_ = std::max(millibits_per_key_, 1);
  }
  // Largest fingerprint for which a table at kMaxLoad fits in the space
  // budget: f = floor(load * bits_per_key). Clamped to the supported range;
  // below the minimum, actual space exceeds the budget.
  int fp_bits = static_cast<int>(int64_t{millibits_per_key_} *
                                 int64_t{kMaxLoadNumerator} /
                                 (int64_t{kMaxLoadDenominator} * 1000));
  fingerprint_bits_ =
      std::min(std::max(fp_bits, kMinFingerprintBits), kMaxFingerprintBits);
}

const char* CuckooFilterPolicy::kClassName() { return "cuckoofilter"; }

const char* CuckooFilterPolicy::kNickName() { return "rocksdb.CuckooFilter"; }

const char* CuckooFilterPolicy::CompatibilityName() const {
  // Persisted in SST files as part of the filter block name. Do not change.
  return "rocksdb.CuckooFilter";
}

std::string CuckooFilterPolicy::GetId() const {
  std::string rv = Name();
  rv += ":" + std::to_string(millibits_per_key_ / 1000);
  int frac = millibits_per_key_ % 1000;
  if (frac > 0) {
    rv.push_back('.');
    rv.push_back(static_cast<char>('0' + (frac / 100)));
    frac %= 100;
    if (frac > 0) {
      rv.push_back(static_cast<char>('0' + (frac / 10)));
      frac %= 10;
      if (frac > 0) {
        rv.push_back(static_cast<char>('0' + frac));
      }
    }
  }
  return rv;
}

FilterBitsBuilder* CuckooFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (millibits_per_key_ == 0) {
    return nullptr;
  }
  return new CuckooFilterBitsBuilder(
      millibits_per_key_, fingerprint_bits_,
      context.table_options.detect_filter_construct_corruption);
}

FilterBitsReader* CuckooFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  return NewCuckooFilterBitsReader(contents);
}

const FilterPolicy* NewCuckooFilterPolicy(double bits_per_key) {
  return new CuckooFilterPolicy(bits_per_key);
}

}  // namespace ROCKSDB_NAMESPACE
