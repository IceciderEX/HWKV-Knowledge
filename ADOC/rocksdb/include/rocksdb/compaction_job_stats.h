// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// 记录单次 flush 操作的关键性能数据
struct FlushMetrics {
  uint64_t total_bytes = 0; // 本次 flsuh 操作写入磁盘的总字节数。
  double memtable_ratio = 0.0; //  可能指 flush 的 MemTable 的空间占用率？
  double write_out_bandwidth = 0.0; // 本次 flush 的写出带宽（例如 MB/s）
  double start_time = 0.0; // 操作开始的时间戳
  int l0_files = 0; // 刷写完成后，L0 层的文件总数
};

// 记录 Compaction 操作的详细性能和状态数据
struct QuicksandMetrics {
  QuicksandMetrics() { Reset(); }
  void Reset();
  int input_level = 0; // 输入和输出层级
  int output_level = 1;
  double drop_ratio = 0.0; // 在压缩过程中被丢弃的数据的比例
  double write_out_bandwidth; // 压缩任务的读带宽
  double read_in_bandwidth; // 压缩任务的写带宽
  int max_bg_compaction; 
  int max_bg_flush;
  double cpu_time_ratio;
  double total_micros;
  double write_amplification; // 本次压缩产生的写放大系数
  uint64_t total_bytes; // 涉及的总数据量
  uint64_t current_pending_bytes; // 发生时，系统中等待压缩的总字节数
  int immu_num; // 发生时，immutable MemTable 数量
  struct op_latency_nanos {
    void Reset() {
      prepare_latency = 0;
      fsync_latency = 0;
      range_latency = 0;
      file_write_latency = 0;
    }
    uint64_t prepare_latency;
    uint64_t fsync_latency;
    uint64_t range_latency;
    uint64_t file_write_latency;
  } io_stat; // 细粒度地记录 I/O 延迟
};

struct CompactionJobStats {
  CompactionJobStats() { Reset(); }
  void Reset();
  // Aggregate the CompactionJobStats from another instance with this one
  void Add(const CompactionJobStats& stats);

  // the elapsed time of this compaction in microseconds.
  uint64_t elapsed_micros;

  // the elapsed CPU time of this compaction in microseconds.
  uint64_t cpu_micros;

  // the number of compaction input records.
  uint64_t num_input_records;
  // the number of blobs read from blob files
  uint64_t num_blobs_read;
  // the number of compaction input files (table files)
  size_t num_input_files;
  // the number of compaction input files at the output level (table files)
  size_t num_input_files_at_output_level;

  // the number of compaction output records.
  uint64_t num_output_records;
  // the number of compaction output files (table files)
  size_t num_output_files;
  // the number of compaction output files (blob files)
  size_t num_output_files_blob;

  // true if the compaction is a full compaction (all live SST files input)
  bool is_full_compaction;
  // true if the compaction is a manual compaction
  bool is_manual_compaction;

  // the total size of table files in the compaction input
  uint64_t total_input_bytes;
  // the total size of blobs read from blob files
  uint64_t total_blob_bytes_read;
  // the total size of table files in the compaction output
  uint64_t total_output_bytes;
  // the total size of blob files in the compaction output
  uint64_t total_output_bytes_blob;

  // number of records being replaced by newer record associated with same key.
  // this could be a new value or a deletion entry for that key so this field
  // sums up all updated and deleted keys
  uint64_t num_records_replaced;

  // the sum of the uncompressed input keys in bytes.
  uint64_t total_input_raw_key_bytes;
  // the sum of the uncompressed input values in bytes.
  uint64_t total_input_raw_value_bytes;

  // the number of deletion entries before compaction. Deletion entries
  // can disappear after compaction because they expired
  uint64_t num_input_deletion_records;
  // number of deletion records that were found obsolete and discarded
  // because it is not possible to delete any more keys with this entry
  // (i.e. all possible deletions resulting from it have been completed)
  uint64_t num_expired_deletion_records;

  // number of corrupt keys (ParseInternalKey returned false when applied to
  // the key) encountered and written out.
  uint64_t num_corrupt_keys;

  // Following counters are only populated if
  // options.report_bg_io_stats = true;

  // Time spent on file's Append() call.
  uint64_t file_write_nanos;

  // Time spent on sync file range.
  uint64_t file_range_sync_nanos;

  // Time spent on file fsync.
  uint64_t file_fsync_nanos;

  // Time spent on preparing file write (fallocate, etc)
  uint64_t file_prepare_write_nanos;

  // 0-terminated strings storing the first 8 bytes of the smallest and
  // largest key in the output.
  static const size_t kMaxPrefixLength = 8;

  std::string smallest_output_key_prefix;
  std::string largest_output_key_prefix;

  // number of single-deletes which do not meet a put
  uint64_t num_single_del_fallthru;

  // number of single-deletes which meet something other than a put
  uint64_t num_single_del_mismatch;

  // TODO: Add output_to_penultimate_level output information
};
}  // namespace ROCKSDB_NAMESPACE
