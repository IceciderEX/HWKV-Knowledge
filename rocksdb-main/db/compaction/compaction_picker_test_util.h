//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <string>
#include <utility>

#include "db/compaction/compaction.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/compaction/file_pri.h"
#include "rocksdb/advanced_options.h"
#include "table/mock_table.h"
#include "table/unique_id_impl.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"


namespace ROCKSDB_NAMESPACE {

class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  size_t log_count;
};

class CompactionPickerTestBase : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  MutableDBOptions mutable_db_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  CountingLogger logger_;
  LogBuffer log_buffer_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  explicit CompactionPickerTestBase(const Comparator* _ucmp)
      : ucmp_(_ucmp),
        icmp_(ucmp_),
        options_(CreateOptions(ucmp_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        mutable_db_options_(),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        log_buffer_(InfoLogLevel::INFO_LEVEL, &logger_),
        file_num_(1),
        vstorage_(nullptr) {
    mutable_cf_options_.ttl = 0;
    mutable_cf_options_.periodic_compaction_seconds = 0;
    // ioptions_.compaction_pri = kMinOverlappingRatio has its own set of
    // tests to cover.
    ioptions_.compaction_pri = kByCompensatedSize;
    fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());
    // When the default value of this option is true, universal compaction
    // tests can encounter assertion failure since SanitizeOption() is
    // not run to set this option to false. So we do the sanitization
    // here. Tests that test this option set this option to true explicitly.
    ioptions_.level_compaction_dynamic_level_bytes = false;
  }

  ~CompactionPickerTestBase() override { ClearFiles(); }

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;
    vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, style, nullptr, false,
        EpochNumberRequirement::kMustPresent, ioptions_.clock,
        options_.bottommost_file_compaction_delay,
        OffpeakTimeOption(mutable_db_options_.daily_offpeak_time_utc)));
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
  }

  // Create a new VersionStorageInfo object so we can add mode files and then
  // merge it with the existing VersionStorageInfo
  void AddVersionStorage() {
    temp_vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, ioptions_.compaction_style,
        vstorage_.get(), false, EpochNumberRequirement::kMustPresent,
        ioptions_.clock, options_.bottommost_file_compaction_delay,
        OffpeakTimeOption(mutable_db_options_.daily_offpeak_time_utc)));
  }

  void DeleteVersionStorage() {
    vstorage_.reset();
    temp_vstorage_.reset();
    ClearFiles();
    file_map_.clear();
    input_files_.clear();
  }

  // REQUIRES: smallest and largest are c-style strings ending with '\0'
  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 1, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           size_t compensated_file_size = 0, bool marked_for_compact = false,
           Temperature temperature = Temperature::kUnknown,
           uint64_t oldest_ancestor_time = kUnknownOldestAncesterTime,
           uint64_t newest_key_time = kUnknownNewestKeyTime,
           Slice ts_of_smallest = Slice(), Slice ts_of_largest = Slice(),
           uint64_t epoch_number = kUnknownEpochNumber) {
    assert(ts_of_smallest.size() == ucmp_->timestamp_size());
    assert(ts_of_largest.size() == ucmp_->timestamp_size());

    VersionStorageInfo* vstorage;
    if (temp_vstorage_) {
      vstorage = temp_vstorage_.get();
    } else {
      vstorage = vstorage_.get();
    }
    assert(level < vstorage->num_levels());
    char* smallest_key_buf = nullptr;
    char* largest_key_buf = nullptr;

    if (!ts_of_smallest.empty()) {
      smallest_key_buf = new char[strlen(smallest) + ucmp_->timestamp_size()];
      memcpy(smallest_key_buf, smallest, strlen(smallest));
      memcpy(smallest_key_buf + strlen(smallest), ts_of_smallest.data(),
             ucmp_->timestamp_size());
      largest_key_buf = new char[strlen(largest) + ucmp_->timestamp_size()];
      memcpy(largest_key_buf, largest, strlen(largest));
      memcpy(largest_key_buf + strlen(largest), ts_of_largest.data(),
             ucmp_->timestamp_size());
    }

    InternalKey smallest_ikey = InternalKey(
        smallest_key_buf ? Slice(smallest_key_buf,
                                 ucmp_->timestamp_size() + strlen(smallest))
                         : smallest,
        smallest_seq, kTypeValue);
    InternalKey largest_ikey = InternalKey(
        largest_key_buf
            ? Slice(largest_key_buf, ucmp_->timestamp_size() + strlen(largest))
            : largest,
        largest_seq, kTypeValue);

    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, smallest_ikey, largest_ikey,
        smallest_seq, largest_seq, marked_for_compact, temperature,
        kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
        kUnknownFileCreationTime, epoch_number, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        true /* user_defined_timestamps_persisted */);
    f->compensated_file_size =
        (compensated_file_size != 0) ? compensated_file_size : file_size;
    // oldest_ancester_time is only used if newest_key_time is not available
    f->oldest_ancester_time = oldest_ancestor_time;
    TableProperties tp;
    tp.newest_key_time = newest_key_time;
    f->fd.table_reader = new mock::MockTableReader(mock::KVVector{}, tp);

    vstorage->AddFile(level, f);
    files_.emplace_back(f);
    file_map_.insert({file_number, {f, level}});

    delete[] smallest_key_buf;
    delete[] largest_key_buf;
  }

  void SetCompactionInputFilesLevels(int level_count, int start_level) {
    input_files_.resize(level_count);
    for (int i = 0; i < level_count; ++i) {
      input_files_[i].level = start_level + i;
    }
    compaction_level_start_ = start_level;
  }

  void AddToCompactionFiles(uint32_t file_number) {
    auto iter = file_map_.find(file_number);
    assert(iter != file_map_.end());
    int level = iter->second.second;
    assert(level < vstorage_->num_levels());
    input_files_[level - compaction_level_start_].files.emplace_back(
        iter->second.first);
  }

  void UpdateVersionStorageInfo() {
    if (temp_vstorage_) {
      VersionBuilder builder(FileOptions(), &ioptions_, nullptr,
                             vstorage_.get(), nullptr);
      ASSERT_OK(builder.SaveTo(temp_vstorage_.get()));
      vstorage_ = std::move(temp_vstorage_);
    }
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    vstorage_->SetFinalized();
  }

 private:
  Options CreateOptions(const Comparator* ucmp) const {
    Options opts;
    opts.comparator = ucmp;
    return opts;
  }

  void ClearFiles() {
    for (auto& file : files_) {
      if (file->fd.table_reader != nullptr) {
        delete file->fd.table_reader;
      }
    }
    files_.clear();
  }

  std::unique_ptr<VersionStorageInfo> temp_vstorage_;
};

}