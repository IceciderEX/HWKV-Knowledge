//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_tier.h"

#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool TierCompactionPicker::NeedsCompaction(const VersionStorageInfo* vstorage) const {
    // ttl 过期的 sst 文件不为空
    if (!vstorage->ExpiredTtlFiles().empty()) {
      return true;
    }
    // 定期 compaction 标记的 sst 文件不为空
    if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
      return true;
    }
    // 最底层 sst 文件标记 compaction 不为空
    if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
      return true;
    }
    // 其他 sst 文件标记 compaction 不为空（不能使用原先的FilesMarkedForCompaction()）
    // if (!vstorage->FilesMarkedForCompaction().empty()) {
    //   return true;
    // }
    // 遍历除了最后一层的所有层
    for (int level = 0; level < vstorage->num_levels() - 1; level++) {
        for (auto* f : vstorage->LevelFiles(level)) {
            if (f->marked_for_compaction && !f->being_compacted) {
                return true;
            }
        }
    }
    // 强制 blob gc 标记的 sst 文件不为空
    if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
      return true;
    }
    // 正常的 tier 策略
    for (int level = 0; level < vstorage->num_levels() - 1; level++) {
        if (vstorage->CompactionScore(level) >= 1) {
            return true;
        }
        int i = 0;
    }
    return false;
}

namespace {

enum class CompactToNextLevel {
  kNo,   // compact to the same level as the input file
  kYes,  // compact to the next level except the last level to the same level
  kSkipLastLevel,  // compact to the next level but skip the last level
};

// A class to build a tier compaction step-by-step.
// add for tier compaction style
class TierCompactionBuilder {
 public:
  TierCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // From `start_level_`, pick files to compact to `output_level_`.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // When total L0 size is small compared to Lbase, try to pick intra-L0
  // compaction starting from the newest L0 file. This helps to prevent
  // L0->Lbase compaction with large write-amp.
  //
  // Returns true iff an intra-L0 compaction is picked.
  // `start_level_inputs_` and `output_level_` will be updated accordingly if
  // a compaction is picked.
  bool PickSizeBasedIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the initial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index,
                                 bool only_expand_right = false);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  // If skip_last_level is true, skip the last level.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      CompactToNextLevel compact_to_next_level);

  // 判断是否为 trivial move
  bool CheckTrivialMove(const std::vector<CompactionInputFiles>& inputs);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;

  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  // CompactionInputFiles 中存储了每个 level 的 filemetadata
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};


// 判断是否有 level 中文件数超过 T 的情况
// 如果没有 level 中文件数超过 T 的情况，就返回 nullptr
// 构造Compaction对象
// add for tier compaction style
Compaction* TierCompactionBuilder::PickCompaction() {
    const int T = mutable_cf_options_.compaction_options_tier.files_per_tier;

    for (int level = 0; level < vstorage_->num_levels() - 1; ++level) {
        const auto& level_files = vstorage_->LevelFiles(level);
        bool triggered_by_size = false;
        bool triggered_by_mark = false;

        // 1. 检查文件数量是否达到阈值 T
        if (T > 0) {
            size_t num_non_compacting_files = 0;
            for (const auto* f : level_files) {
                if (!f->being_compacted) {
                    num_non_compacting_files++;
                }
            }
            if (num_non_compacting_files >= static_cast<size_t>(T)) {
                triggered_by_size = true;
            }
        }
        
        // 2. 检查是否有文件被标记
        if (!triggered_by_size) {
            for (const auto* f : level_files) {
                if (f->marked_for_compaction && !f->being_compacted) {
                    triggered_by_mark = true;
                    break;
                }
            }
        }

        // 如果当前层级被触发，则准备进行合并
        if (triggered_by_size || triggered_by_mark) {
            start_level_ = level;
            output_level_ = start_level_ + 1;
            
            // 准备输入文件列表 (compaction_inputs_)
            compaction_inputs_.resize(1);
            compaction_inputs_[0].level = start_level_;

            // 检查该层是否有任何文件正在被合并，如果有，则不能选择这一层
            bool is_busy = false;
            for (FileMetaData* file : level_files) {
                if (file->being_compacted) {
                    is_busy = true;
                    break;
                }
                compaction_inputs_[0].files.emplace_back(file);
            }

            if (is_busy) {
                compaction_inputs_.clear();
                continue;
            }

            // 检查输出冲突
            if (compaction_picker_->FilesRangeOverlapWithCompaction(
                    compaction_inputs_, output_level_, Compaction::kInvalidLevel)) {
                compaction_inputs_.clear();
                continue;
            }
            
            // 设置一下原因
            compaction_reason_ = triggered_by_size
                                     ? CompactionReason::kLevelFilesNum
                                     : CompactionReason::kFilesMarkedForCompaction;
            
            Compaction* c = GetCompaction();
            TEST_SYNC_POINT_CALLBACK("TierCompactionPicker::PickCompaction:Return", c);
            return c;
        }
    }

    // 没有找到任何需要合并的层
    return nullptr;
}

Compaction* TierCompactionBuilder::GetCompaction() {
  assert(!compaction_inputs_.empty());
  bool l0_files_might_overlap =
      start_level_ == 0 && !is_l0_trivial_move_ &&
      (compaction_inputs_.size() > 1 || compaction_inputs_[0].size() > 1);
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_),
      /* earliest_snapshot */ std::nullopt, /* snapshot_checker */ nullptr,
      compaction_reason_,
      /* trim_ts */ "", start_level_score_, l0_files_might_overlap);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t TierCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

}  // namespace

Compaction* TierCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots */,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool /* require_max_output_level*/) {
  TierCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_,
                                 mutable_db_options);
  return builder.PickCompaction();
}

}  // namespace ROCKSDB_NAMESPACE
