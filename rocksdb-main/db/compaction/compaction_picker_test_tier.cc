// 在 db/compaction/compaction_picker_tier_test.cc

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
#include "db/compaction/compaction_picker_tier.h" 
#include "db/compaction/compaction_picker_test_util.h" 
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

// 创建一个专门用于 Tiering 策略测试的类
class TieringCompactionPickerTest : public CompactionPickerTestBase {
 public:
  TieringCompactionPickerTest() : CompactionPickerTestBase(BytewiseComparator()) {
    // 在构造函数中明确设置 Compaction 策略
    ioptions_.compaction_style = kCompactionStyleTier;
    // 实例化你的 Tiering Picker
    picker_.reset(new TierCompactionPicker(ioptions_, &icmp_));
  }

  std::unique_ptr<TierCompactionPicker> picker_;
};

// --- NeedsCompaction() 的测试 ---

// 测试：当没有任何层级达到阈值 T 时，不应该触发 Compaction
TEST_F(TieringCompactionPickerTest, NeedsCompaction_BelowThreshold) {
  // 1. Arrange (准备环境)
  const int T = 4;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(6, kCompactionStyleTier);
  // 向 L1 添加 T-1 个文件
  Add(1, 1, "a", "b");
  Add(1, 2, "c", "d");
  Add(1, 3, "e", "f");
  UpdateVersionStorageInfo();

  // 2. Act (执行操作)
  bool needs_compaction = picker_->NeedsCompaction(vstorage_.get());

  // 3. Assert (断言结果)
  ASSERT_FALSE(needs_compaction);
}

// 测试：当某个层级刚好达到阈值 T 时，应该触发 Compaction
TEST_F(TieringCompactionPickerTest, NeedsCompaction_AtThreshold) {
  // 1. Arrange
  const int T = 4;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(6, kCompactionStyleTier);
  // 向 L1 添加 T 个文件
  Add(1, 1, "a", "b");
  Add(1, 2, "c", "d");
  Add(1, 3, "e", "f");
  Add(1, 4, "g", "h");
  UpdateVersionStorageInfo();

  // 2. Act
  bool needs_compaction = picker_->NeedsCompaction(vstorage_.get());

  // 3. Assert
  ASSERT_TRUE(needs_compaction);
}

// 测试：即使最底层文件数达到阈值，也不应该触发 Compaction
TEST_F(TieringCompactionPickerTest, NeedsCompaction_LastLevelFull) {
  // 1. Arrange
  const int T = 4;
  const int kNumLevels = 6;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(kNumLevels, kCompactionStyleTier);
  
  // 向最底层 (L5) 添加 T 个文件
  Add(kNumLevels - 1, 1, "a", "b");
  Add(kNumLevels - 1, 2, "c", "d");
  Add(kNumLevels - 1, 3, "e", "f");
  Add(kNumLevels - 1, 4, "g", "h");
  UpdateVersionStorageInfo();

  // 2. Act
  bool needs_compaction = picker_->NeedsCompaction(vstorage_.get());

  // 3. Assert
  ASSERT_FALSE(needs_compaction); // 最底层无法作为输入层
}

// --- PickCompaction() 的测试 ---

// 测试：当 L1 和 L2 都满了，应该优先选择 L1 (最低的满员层级)
TEST_F(TieringCompactionPickerTest, PickCompaction_PicksLowestFullLevel) {
  // 1. Arrange
  const int T = 3;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(6, kCompactionStyleTier);
  // L1 满
  Add(1, 1, "a", "b");
  Add(1, 2, "c", "d");
  Add(1, 3, "e", "f");
  // L2 也满
  Add(2, 4, "g", "h");
  Add(2, 5, "i", "j");
  Add(2, 6, "k", "l");
  UpdateVersionStorageInfo();

  // 2. Act
  std::unique_ptr<Compaction> compaction(picker_->PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, {}, nullptr, vstorage_.get(),
      &log_buffer_));

  // 3. Assert
  ASSERT_NE(compaction, nullptr);
  ASSERT_EQ(compaction->start_level(), 1); // 确认选择了 L1
  ASSERT_EQ(compaction->output_level(), 2);
}

// 测试：确认挑选了目标层级的所有文件作为输入
TEST_F(TieringCompactionPickerTest, PickCompaction_SelectsAllFilesFromLevel) {
  // 1. Arrange
  const int T = 3;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(6, kCompactionStyleTier);
  // L1 满
  Add(1, 10, "a", "b");
  Add(1, 11, "c", "d");
  Add(1, 12, "e", "f");
  UpdateVersionStorageInfo();

  // 2. Act
  std::unique_ptr<Compaction> compaction(picker_->PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, {}, nullptr, vstorage_.get(),
      &log_buffer_));

  // 3. Assert
  ASSERT_NE(compaction, nullptr);
  ASSERT_EQ(compaction->num_input_files(0), 3); // 确认输入文件数量是3
  ASSERT_EQ(compaction->input(0, 0)->fd.GetNumber(), 10);
  ASSERT_EQ(compaction->input(0, 1)->fd.GetNumber(), 11);
  ASSERT_EQ(compaction->input(0, 2)->fd.GetNumber(), 12);
}

// 测试：如果目标层级中有文件正在被合并，则本次不应挑选出 Compaction 任务
TEST_F(TieringCompactionPickerTest, PickCompaction_AbortsIfFileIsBusy) {
  // 1. Arrange
  const int T = 3;
  mutable_cf_options_.compaction_options_tier.files_per_tier = T;
  NewVersionStorage(6, kCompactionStyleTier);
  // L1 满
  Add(1, 10, "a", "b");
  Add(1, 11, "c", "d");
  Add(1, 12, "e", "f");
  // 手动将会被选中的文件之一标记为 "being_compacted"
  file_map_[11].first->being_compacted = true;
  UpdateVersionStorageInfo();

  // 2. Act
  std::unique_ptr<Compaction> compaction(picker_->PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, {}, nullptr, vstorage_.get(),
      &log_buffer_));

  // 3. Assert
  ASSERT_EQ(compaction, nullptr); // 因为有文件被占用，不应生成 Compaction 任务
}

}