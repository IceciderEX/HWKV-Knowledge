#include "rocksdb/utilities/adoc_tuner.h"

namespace ROCKSDB_NAMESPACE {

DOTA_Tuner::~DOTA_Tuner() = default;

SystemScores SystemScores::operator+(const SystemScores& a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed - a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio - a.active_size_ratio;
  temp.immutable_number = this->immutable_number - a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg - a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var - a.flush_speed_var;
  temp.l0_num = this->l0_num - a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio - a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes - a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth - a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time - a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time - a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time - a.flush_gap_time;
  temp.flush_numbers = this->flush_numbers - a.flush_numbers;

  return temp;
}

SystemScores SystemScores::operator-(const SystemScores& a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed - a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio - a.active_size_ratio;
  temp.immutable_number = this->immutable_number - a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg - a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var - a.flush_speed_var;
  temp.l0_num = this->l0_num - a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio - a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes - a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth - a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time - a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time - a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time - a.flush_gap_time;
  temp.flush_numbers = this->flush_numbers - a.flush_numbers;

  return temp;
}

SystemScores SystemScores::operator/(const int& a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed / a;
  temp.active_size_ratio = this->active_size_ratio / a;
  temp.immutable_number = this->immutable_number / a;
  temp.flush_speed_avg = this->flush_speed_avg / a;
  temp.flush_speed_var = this->flush_speed_var / a;
  temp.l0_num = this->l0_num / a;
  temp.l0_drop_ratio = this->l0_drop_ratio / a;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes / a;
  temp.disk_bandwidth = this->disk_bandwidth / a;
  temp.compaction_idle_time =
      this->compaction_idle_time / a;
  temp.flush_idle_time = this->flush_idle_time / a;
  temp.flush_gap_time = this->flush_gap_time / a;
  temp.flush_numbers = this->flush_numbers / a;

  return temp;
}

SystemScores DOTA_Tuner::ScoreTheSystem() {
    // 更新指向 RocksDB 内部数据结构的指针，获取最新的系统指标
    UpdateSystemStats();
    SystemScores current_score;

    // 1. 获取与内存相关的指标
    uint64_t total_mem_size = 0;
    uint64_t active_mem_size = 0;
    // 1.(A)使用 RocksDB 提供的 API 获取内存使用情况
    running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
    running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem_size);
    // 计算 Active Memtable 的占用率
    current_score.active_size_ratio = static_cast<double>(active_mem_size) / total_mem_size;
    // 1.(B)获取等待刷写的 immutable memtable 的数量，用于判断 MMO
    current_score.immutable_number = cfd->imm() == nullptr ? 0 : cfd->imm()->NumNotFlushed();
    // 1.(C)计算 memtable 的写入速率
    // 通过比较当前与上次的 memtable 总大小来估算
    // 最后转换为 MB/gap 单位
    while (total_mem_size < last_unflushed_bytes) {
        total_mem_size += current_options.write_buffer_size;
    }
    current_score.memtable_speed += (total_mem_size - last_unflushed_bytes);
    last_unflushed_bytes = total_mem_size;
    current_score.memtable_speed /= tuning_gap;
    current_score.memtable_speed /= (kMicrosInSecond); // 实际上是 1000*1000

    // 2. 获取 L0 与待压缩数据的相关指标
    // level0_slowdown_writes_trigger：L0 的 memtable 数量达到时，RocksDB 将开始主动减慢前台的写入速度
    // soft_pending_compaction_bytes_limit：待压缩字节数达到这个值时，RocksDB 将开始主动减慢前台的写入速度
    current_score.l0_num = static_cast<double>(vfs->NumLevelFiles(vfs->base_level())) /
                        current_options.level0_slowdown_writes_trigger;
    current_score.estimate_compaction_bytes = static_cast<double>(vfs->estimated_compaction_needed_bytes()) / 
                        current_options.soft_pending_compaction_bytes_limit;
    
    // 3. 后台任务（flush/compaction）的详细指标
    auto flush_result_length = running_db_->immutable_db_options().flush_stats->size();
    auto compaction_result_length = running_db_->immutable_db_options().job_stats->size();

    std::vector<FlushMetrics> flush_metric_list;
    // 遍历自上次检查以来新增的 Flush 事件
    for (uint64_t i = flush_list_accessed; i < flush_result_length; i++) {
        auto temp = flush_list_from_opt_ptr->at(i);
        current_score.flush_min = std::min(current_score.flush_speed_avg, current_score.flush_min);
        flush_metric_list.push_back(temp);
        // 将每次 Flush 的写出带宽累加起来，用于评估 MMO
        current_score.flush_speed_avg += temp.write_out_bandwidth;
        // Flush 写入的字节数累加到总的磁盘带宽消耗中
        current_score.disk_bandwidth += temp.total_bytes;
        last_non_zero_flush = temp.write_out_bandwidth;
        if (current_score.l0_num > temp.l0_files) {
            current_score.l0_num = temp.l0_files;
        }
    }

    int l0_compaction = 0;
    uint64_t max_pending_bytes = 0;
    last_unflushed_bytes = total_mem_size;
    // 遍历自上次检查以来新增的 Compaction 事件
    for (uint64_t i = compaction_list_accessed; i < compaction_result_length; i++) {
        auto temp = compaction_list_from_opt_ptr->at(i);
        // 记录 l0 压缩的次数
        if (temp.input_level == 0) {
            current_score.l0_drop_ratio += temp.drop_ratio;
            l0_compaction++;
        }
        // 寻找本周期内出现过的最大的待压缩字节数峰值 
        if (temp.current_pending_bytes > max_pending_bytes) {
            max_pending_bytes = temp.current_pending_bytes;
        }
        current_score.disk_bandwidth += temp.total_bytes;
    }
    current_score.disk_bandwidth /= kMicrosInSecond;
    current_score.estimate_compaction_bytes = static_cast<double>(vfs->estimated_compaction_needed_bytes()) /
        current_options.soft_pending_compaction_bytes_limit;

    // flush_speed_avg, flush_speed_var, l0_drop_ratio
    auto num_new_flushes = (flush_result_length - flush_list_accessed);
    current_score.flush_numbers = num_new_flushes;
    // 如果在本次调优周期内有新的 flush 操作发生 (num_new_flushes > 0)
    if (num_new_flushes != 0) {
        // 本次周期内所有 flush 操作的平均带宽
        // 体现 Memtable 持久化到磁盘的效率
        auto avg_flush = current_score.flush_speed_avg / num_new_flushes;
        current_score.flush_speed_avg /= num_new_flushes;
        for (auto item : flush_metric_list) {
            // 计算方差
            current_score.flush_speed_var += (item.write_out_bandwidth - avg_flush) *
                                            (item.write_out_bandwidth - avg_flush);
        }
        current_score.flush_speed_var /= num_new_flushes;
        current_score.flush_gap_time /= (kMicrosInSecond * num_new_flushes);
    }

    // 计算 L0 层压缩的丢弃率（判断写入的数据是否很快过时？）
    if (l0_compaction != 0) {
        current_score.l0_drop_ratio /= l0_compaction;
    }

    // 获取后台线程池的等待时间
    // Env::HIGH 对应的是 flush 线程池，Env::LOW 对应的是 compaction 线程池
    auto flush_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::HIGH);
    auto compaction_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::LOW);
    std::unordered_map<int, uint64_t> thread_idle_time;
    // 本次调优周期内新增的 flush 线程等待记录
    // 下面分别累加了 flush 与 compaction 的空闲时间
    uint64_t temp = flush_thread_idle_list.size();
    for (uint64_t i = last_flush_thread_len; i < temp; i++) {
        auto temp_entry = flush_thread_idle_list[i];
        auto value = temp_entry.second;
        current_score.flush_idle_time += value;
    }
    temp = compaction_thread_idle_list.size();
    for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
        auto temp_entry = compaction_thread_idle_list[i];
        auto value = temp_entry.second;
        current_score.compaction_idle_time += value;
    }
    // 分别转换为 idle time 占总时间的比例
    // 通过直接计算线程的 idle time 来判断后台任务是否饱和
    current_score.flush_idle_time /=
        (current_options.max_background_jobs * kMicrosInSecond / 4);
    // flush threads always get 1/4 of all
    current_score.compaction_idle_time /=
        (current_options.max_background_jobs * kMicrosInSecond * 3 / 4);

    // clean up
    // 更新索引
    flush_list_accessed = flush_result_length;
    compaction_list_accessed = compaction_result_length;
    last_compaction_thread_len = compaction_thread_idle_list.size();
    last_flush_thread_len = flush_thread_idle_list.size();
    return current_score;
}

void DOTA_Tuner::DetectTuningOperations(int secs_elapsed, std::vector<ChangePoint> *change_list_ptr) {
  current_sec = secs_elapsed;
  // UpdateSystemStats();
  SystemScores current_score = ScoreTheSystem();
  UpdateMaxScore(current_score);
  scores.push_back(current_score);
  // 梯度计算：最近的变化量
  gradients.push_back(current_score - scores.front());

  auto thread_stat = LocateThreadStates(current_score);
  auto batch_stat = LocateBatchStates(current_score);

  AdjustmentTuning(change_list_ptr, current_score, thread_stat, batch_stat);
  // decide the operation based on the best behavior and last behavior
  // update the histories
  last_thread_states = thread_stat;
  last_batch_stat = batch_stat;
  tuning_rounds++;
}

void DOTA_Tuner::AdjustmentTuning(std::vector<ChangePoint> *change_list,
                                  SystemScores &score,
                                  ThreadOverflowLevels thread_levels,
                                  BatchSizeOverflowLevels batch_levels) {
  auto tuning_op = VoteForOP(score, thread_levels, batch_levels);
  // 根据 op 去构造 changepoint
  FillUpChangeList(change_list, tuning_op);
}

ThreadOverflowLevels DOTA_Tuner::LocateThreadStates(SystemScores &score) {
  // 系统写入吞吐量是否发生了显著下降？（小于最佳的0.7倍）  
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    // speed is slower than before, performance is in the stall area
    if (score.immutable_number >= 1) { // 存在等待刷盘的 Memtable
      if (score.flush_speed_avg <= max_scores.flush_speed_avg * 0.5) { // 且刷盘速度很慢
        // it's not influenced by the flushing speed
        if (current_options.max_background_jobs > 6) { // 如果线程数已经很多了
            // 对应 MMO
            return kMemtableOverflow; 
        }
      } else if (score.l0_num > 0.5) {
        // 对应 L0O
        return kL0Overflow;
      }
    } else if (score.l0_num > 0.7) {
      // 对应 L0O
      return kL0Overflow;
    } else if (score.estimate_compaction_bytes > 0.5) {
      // 对应 RDO
      return kRedundancyDataOverflow;
    }
  } else if (score.compaction_idle_time > 2.5) { // 没有出现写停顿并发现压缩线程的空闲时间比例非常高（>2.5倍的工作时间）
    return kIdle;
  }
  // 状态良好
  return kGoodCondition;
}

BatchSizeOverflowLevels DOTA_Tuner::LocateBatchStates(SystemScores &score) {
  // 与前面类似，如果写入性能下降
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    if (score.flush_speed_avg < max_scores.flush_speed_avg * 0.5) { // 且刷盘慢
      if (score.active_size_ratio > 0.5 && score.immutable_number >= 1)  { 
        // active Memtable 的占用过半，而前一个 Memtable 还没刷完
        // 说明 memtable 的大小过于小
        return kTinyMemtable;
      } else if (current_options.max_background_jobs > 6 || score.l0_num > 0.9) {
        // 后台任务重（线程多或 L0 积压）的情况下，需要增加 memtable 大小
        // 增加 batchsize 提高单次 flush 效率
        return kTinyMemtable;
      }
    }
  } else if (score.flush_numbers < max_scores.flush_numbers * 0.3) {
    // 系统性能正常，但是刷盘次数相比历史峰值下降很多
    // 可能 memtable 大小设置的过大导致刷盘操作不频繁
    return kFlushDecrease;
  }
  // 正常状态
  return kOverflowFree;
};

TuningOP DOTA_Tuner::VoteForOP(SystemScores & /*current_score*/,
                               ThreadOverflowLevels thread_level,
                               BatchSizeOverflowLevels batch_level) {
  TuningOP op;
  // 优先顺序：L0O > RDO > MMO
  switch (thread_level) {
    case kL0Overflow:
      op.ThreadOp = kLinearIncrease;
      break;
    case kRedundancyDataOverflow:
      op.ThreadOp = kLinearIncrease;
      break;
    case kGoodCondition:
      op.ThreadOp = kKeep;
      break;
    case kIdle:
      op.ThreadOp = kHalf;
      break;
    case kMemtableOverflow:
      op.ThreadOp = kHalf;
      break;
  }

  if (batch_level == kTinyMemtable) {
    op.BatchOp = kLinearIncrease;
  } else if (batch_level == kOverflowFree) {
    op.BatchOp = kKeep;
  } else {
    op.BatchOp = kHalf;
  }

  return op;
}

void DOTA_Tuner::FillUpChangeList(std::vector<ChangePoint> *change_list,
                                  TuningOP op) {
  uint64_t current_thread_num = current_options.max_background_jobs;
  uint64_t current_batch_size = current_options.write_buffer_size;
  // 根据 batch operation 设置 batchsize
  // 遵循加性增，乘性减的原则
  switch (op.BatchOp) {
    case kLinearIncrease:
      SetBatchSize(change_list, current_batch_size += default_options.write_buffer_size);
      break;
    case kHalf:
      SetBatchSize(change_list, current_batch_size /= 2);
      break;
    case kKeep:
      break;
  }
  // 根据 thread operation 设置 thread number
  switch (op.ThreadOp) {
    case kLinearIncrease:
      SetThreadNum(change_list, current_thread_num += 2);
      break;
    case kHalf:
      SetThreadNum(change_list, current_thread_num /= 2);
      break;
    case kKeep:
      break;
  }
}

inline void DOTA_Tuner::SetThreadNum(std::vector<ChangePoint> *change_list, int target_value) {
  ChangePoint thread_num_cp;
  thread_num_cp.option = max_bg_jobs;
  thread_num_cp.is_db_level = true;
  target_value = std::max(target_value, min_thread);
  target_value = std::min(target_value, max_thread);
  thread_num_cp.value = std::to_string(target_value);
  change_list->push_back(thread_num_cp);
}

inline void DOTA_Tuner::SetBatchSize(std::vector<ChangePoint> *change_list,
                                     uint64_t target_value) {
  ChangePoint memtable_size_cp;
  ChangePoint L1_total_size;
  ChangePoint sst_size_cp;
  //  ChangePoint write_buffer_number;

  sst_size_cp.option = sst_size;
  L1_total_size.option = total_l1_size;
  // adjust the memtable size
  memtable_size_cp.is_db_level = false;
  memtable_size_cp.option = memtable_size;

  target_value = std::max(target_value, min_memtable_size);
  target_value = std::min(target_value, max_memtable_size);

  // SST sizes should be controlled to be the same as memtable size
  memtable_size_cp.value = std::to_string(target_value);
  sst_size_cp.value = std::to_string(target_value);

  // calculate the total size of L1
  uint64_t l1_size = current_options.level0_file_num_compaction_trigger *
                     current_options.min_write_buffer_number_to_merge *
                     target_value;

  L1_total_size.value = std::to_string(l1_size);
  sst_size_cp.is_db_level = false;
  L1_total_size.is_db_level = false;

  //  change_list->push_back(write_buffer_number);
  change_list->push_back(memtable_size_cp);
  change_list->push_back(L1_total_size);
  change_list->push_back(sst_size_cp);
}

} // namespace ROCKSDB_NAMESPACE