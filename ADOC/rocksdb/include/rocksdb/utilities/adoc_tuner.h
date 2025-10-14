#pragma once

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

// 与后台线程数相关的系统瓶颈状态
enum ThreadOverflowLevels {
    kL0Overflow,
    kRedundancyDataOverflow,
    kGoodCondition,
    kIdle,
    kMemtableOverflow,
};

// 与批量大小（memtable size）相关的系统瓶颈状态
enum BatchSizeOverflowLevels {
    kTinyMemtable,
    kOverflowFree,
    kFlushDecrease,
};

// 每次时间窗口结束之后，系统的状态的数据
// 量化系统在某一时刻的数据
// 每个成员都直接对应 DOTA_tuner.cc 中 ScoreTheSystem 函数采集的指标
struct SystemScores {
  // Memory Component
  uint64_t memtable_speed;   // MB per sec
  double active_size_ratio;  // active size / total memtable size
  int immutable_number;      // NonFlush number
  // Flushing
  double flush_speed_avg;
  double flush_min;
  double flush_speed_var;
  // Compaction speed
  double l0_num;
  // LSM size
  double l0_drop_ratio;
  double estimate_compaction_bytes;  // given by the system, divided by the soft limit
  // System metrics
  double disk_bandwidth;  // avg
  double flush_idle_time;
  double flush_gap_time;
  double compaction_idle_time;  // calculate by idle calculating,flush and compaction stats separately
  int flush_numbers;

  // 初始化所有指标为0
  SystemScores() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_min = 9999999;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  
  // 重置函数
  void Reset() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  
  // 运算符重载，方便进行分数计算（如计算梯度）

  // 减法重载，计算性能梯度的基础
  SystemScores operator-(const SystemScores& a);
  // 加法与除法重载，用于计算平均梯度
  SystemScores operator+(const SystemScores& a);
  SystemScores operator/(const int& a);
};

typedef SystemScores ScoreGradient;

// 用于表示一个具体的配置变更
struct ChangePoint {
  std::string option;
  std::string value;
  int change_timing;
  // 是否是数据库实例级别的或者是列族级别的
  bool is_db_level;
};

// 定义了调优操作的类型，对应论文中的 AIMD 算法
enum OpType : int { kLinearIncrease, kHalf, kKeep };

// 将对批处理大小和线程数的调优决策组合在一起
struct TuningOP {
  OpType BatchOp;
  OpType ThreadOp;
};

class DOTA_Tuner {
 protected:
  const Options default_options; // rocksdb 的默认配置
  uint64_t tuning_rounds; // 计数器（轮次）
  Options current_options; // 正在生效的配置
  // 指向 rocksdb 内部数据结构的指针
  Version* version;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;

  DBImpl* running_db_;
  int64_t* last_report_ptr;
  std::atomic<int64_t>* total_ops_done_ptr_;
  // 历史数据队列
  std::deque<SystemScores> scores;
  std::vector<ScoreGradient> gradients;
  int current_sec;
  // 最近一次调优周期的线程和批量大小状态
  ThreadOverflowLevels last_thread_states;
  BatchSizeOverflowLevels last_batch_stat;
  // 存储 flush 与 compaction 的详细 metric 列表
  uint64_t flush_list_accessed, compaction_list_accessed;
  std::shared_ptr<std::vector<FlushMetrics>> flush_list_from_opt_ptr;
  std::shared_ptr<std::vector<QuicksandMetrics>> compaction_list_from_opt_ptr;
  SystemScores max_scores; // 历史峰值
  SystemScores avg_scores; // 平均值

  uint64_t last_flush_thread_len;
  uint64_t last_compaction_thread_len;
  Env* env_;

  double tuning_gap; // tuning 的时间间隔
  uint64_t last_unflushed_bytes = 0; // 上周期的 memtable 字节数
  uint64_t last_non_zero_flush = 0; // 最近一次非零 flush 的带宽
  
  int double_ratio = 2;
  const int score_array_len = 600 / tuning_gap;
  double idle_threshold = 2.5;
  double FEA_gap_threshold = 1;
  double TEA_slow_flush = 0.5;

  void UpdateSystemStats() { UpdateSystemStats(running_db_); }

 public:
  DOTA_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env,
             uint64_t gap_sec)
      : default_options(opt),
        tuning_rounds(0),
        running_db_(running_db),
        scores(),
        gradients(0),
        current_sec(0),
        last_thread_states(kL0Overflow),
        last_batch_stat(kTinyMemtable),
        flush_list_accessed(0),
        compaction_list_accessed(0),
        flush_list_from_opt_ptr(running_db->immutable_db_options().flush_stats),
        compaction_list_from_opt_ptr(
            running_db->immutable_db_options().job_stats),
        max_scores(),
        last_flush_thread_len(0),
        last_compaction_thread_len(0),
        env_(env),
        tuning_gap(gap_sec),
        core_num(running_db->immutable_db_options().core_number),
        max_memtable_size(
            running_db->immutable_db_options().max_memtable_size) {
    this->last_report_ptr = last_report_op_ptr;
    this->total_ops_done_ptr_ = total_ops_done_ptr;
  }
  void set_idle_ratio(double idle_ra) { idle_threshold = idle_ra; }
  void set_gap_threshold(double ng_threshold) {
    FEA_gap_threshold = ng_threshold;
  }
  void set_slow_flush_threshold(double sf_threshold) {
    this->TEA_slow_flush = sf_threshold;
  }
  virtual ~DOTA_Tuner();

  // 更新 max_scores 中的各项数据，用于记录历史峰值
  inline void UpdateMaxScore(SystemScores& current_score) {
    if (current_score.memtable_speed > max_scores.memtable_speed) {
      max_scores.memtable_speed = current_score.memtable_speed;
    }
    if (current_score.active_size_ratio > max_scores.active_size_ratio) {
      max_scores.active_size_ratio = current_score.active_size_ratio;
    }
    if (current_score.immutable_number > max_scores.immutable_number) {
      max_scores.immutable_number = current_score.immutable_number;
    }

    if (current_score.flush_speed_avg > max_scores.flush_speed_avg) {
      max_scores.flush_speed_avg = current_score.flush_speed_avg;
    }
    if (current_score.flush_speed_var > max_scores.flush_speed_var) {
      max_scores.flush_speed_var = current_score.flush_speed_var;
    }
    if (current_score.l0_num > max_scores.l0_num) {
      max_scores.l0_num = current_score.l0_num;
    }
    if (current_score.l0_drop_ratio > max_scores.l0_drop_ratio) {
      max_scores.l0_drop_ratio = current_score.l0_drop_ratio;
    }
    if (current_score.estimate_compaction_bytes >
        max_scores.estimate_compaction_bytes) {
      max_scores.estimate_compaction_bytes =
          current_score.estimate_compaction_bytes;
    }
    if (current_score.disk_bandwidth > max_scores.disk_bandwidth) {
      max_scores.disk_bandwidth = current_score.disk_bandwidth;
    }
    if (current_score.flush_idle_time > max_scores.flush_idle_time) {
      max_scores.flush_idle_time = current_score.flush_idle_time;
    }
    if (current_score.compaction_idle_time > max_scores.compaction_idle_time) {
      max_scores.compaction_idle_time = current_score.compaction_idle_time;
    }
    if (current_score.flush_numbers > max_scores.flush_numbers) {
      max_scores.flush_numbers = current_score.flush_numbers;
    }
  }

  void ResetTuner() { tuning_rounds = 0; }
  // 通过实例 running_db 更新 current_options 与 version
  void UpdateSystemStats(DBImpl* running_db) {
    current_options = running_db->GetOptions();
    version = running_db->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }
  // 
  virtual void DetectTuningOperations(int secs_elapsed, std::vector<ChangePoint>* change_list);

  ScoreGradient CompareWithBefore() { return scores.back() - scores.front(); }
  ScoreGradient CompareWithBefore(SystemScores& past_score) {
    return scores.back() - past_score;
  }
  ScoreGradient CompareWithBefore(SystemScores& past_score,
                                  SystemScores& current_score) {
    return current_score - past_score;
  }
  // 判断当前的线程状态是否合适，返回线程状态
  ThreadOverflowLevels LocateThreadStates(SystemScores& score);
  // 判断当前的批量大小状态是否合适，返回批量大小状态
  BatchSizeOverflowLevels LocateBatchStates(SystemScores& score);

  const std::string memtable_size = "write_buffer_size";
  const std::string sst_size = "target_file_size_base";
  const std::string total_l1_size = "max_bytes_for_level_base";
  const std::string max_bg_jobs = "max_background_jobs";
  const std::string memtable_number = "max_write_buffer_number";

  // 调优参数边界值
  const int core_num;
  int max_thread = core_num;
  const int min_thread = 2;
  uint64_t max_memtable_size;
  const uint64_t min_memtable_size = 64 << 20;

  SystemScores ScoreTheSystem();
  void AdjustmentTuning(std::vector<ChangePoint>* change_list,
                        SystemScores& score, ThreadOverflowLevels levels,
                        BatchSizeOverflowLevels stallLevels);
  // 接收 Locate... 函数的返回值，根据当前系统状态决定调优操作
  TuningOP VoteForOP(SystemScores& current_score, ThreadOverflowLevels levels,
                     BatchSizeOverflowLevels stallLevels);
  // 将 op 转换为具体的调优操作（ChangePoint）
  void FillUpChangeList(std::vector<ChangePoint>* change_list, TuningOP op);
  // 具体的修改批量大小
  void SetBatchSize(std::vector<ChangePoint>* change_list,
                    uint64_t target_value);
  // 具体的修改线程数
  void SetThreadNum(std::vector<ChangePoint>* change_list, int target_value);
};

enum Stage : int { kSlowStart, kStabilizing };
class FEAT_Tuner : public DOTA_Tuner {
 public:
  FEAT_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env, int gap_sec,
             bool triggerTEA, bool triggerFEA)
      : DOTA_Tuner(opt, running_db, last_report_op_ptr, total_ops_done_ptr, env,
                   gap_sec),
        TEA_enable(triggerTEA),
        FEA_enable(triggerFEA),
        current_stage(kSlowStart) {
    flush_list_from_opt_ptr =
        this->running_db_->immutable_db_options().flush_stats;

    std::cout << "Using FEAT tuner.\n FEA is "
              << (FEA_enable ? "triggered" : "NOT triggered") << std::endl;
    std::cout << "TEA is " << (TEA_enable ? "triggered" : "NOT triggered")
              << std::endl;
  }
  void DetectTuningOperations(int secs_elapsed,
                              std::vector<ChangePoint>* change_list) override;
  ~FEAT_Tuner() override;

  TuningOP TuneByTEA();
  TuningOP TuneByFEA();

 private:
  bool TEA_enable;
  bool FEA_enable;
  SystemScores current_score_;
  SystemScores head_score_;
  std::deque<TuningOP> recent_ops;
  Stage current_stage;
  double bandwidth_congestion_threshold = 0.7;
  double slow_down_threshold = 0.75;
  double RO_threshold = 0.8;
  double LO_threshold = 0.7;
  double MO_threshold = 0.5;
  double batch_changing_frequency = 0.7;
  int congestion_threads = min_thread;
  //  int double_ratio = 4;
  SystemScores normalize(SystemScores& origin_score);

  inline const char* StageString(Stage v) {
    switch (v) {
      case kSlowStart:
        return "slow start";
        //      case kBoundaryDetection:
        //        return "Boundary Detection";
      case kStabilizing:
        return "Stabilizing";
    }
    return "unknown operation";
  }
  void CalculateAvgScore();
};
inline const char* OpString(OpType v) {
  switch (v) {
    case kLinearIncrease:
      return "Linear Increase";
    case kHalf:
      return "Half";
    case kKeep:
      return "Keep";
  }
  return "unknown operation";
}

} // namespace ROCKSDB_NAMESPACE