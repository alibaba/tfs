#include "stat_tool.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::tools;

ValueRange::ValueRange(const int32_t min_value, const int32_t max_value)
  : min_value_(min_value), max_value_(max_value), count_(0)
{
}
ValueRange::~ValueRange()
{
  count_ = 0;
}
bool ValueRange::is_in_range(const int32_t value) const
{
  bool ret = false;
  if (max_value_ == -1)
  {
    if (value >= min_value_)
    {
      ret = true;
    }
  }
  else if (min_value_ <= max_value_)
  {
    if ((value >= min_value_) && (value < max_value_))
    {
      ret = true;
    }
  }
  return ret;
}

void ValueRange::incr()
{
  count_++;
}

void ValueRange::decr()
{
  count_--;
}

BlockSizeRange::BlockSizeRange(const int32_t min_value, const int32_t max_value)
  :ValueRange(min_value, max_value)
{
}

BlockSizeRange::~BlockSizeRange()
{
}

void BlockSizeRange::dump(FILE* fp) const
{
  if (max_value_ != -1)
  {
    fprintf(fp, "%dM ~ %dM: %"PRI64_PREFIX"d\n", min_value_, max_value_, count_);
  }
  else
  {
    fprintf(fp, "%dM ~ : %"PRI64_PREFIX"d\n", min_value_, count_);
  }
}

DelBlockRange::DelBlockRange(const int32_t min_value, const int32_t max_value)
  : ValueRange(min_value, max_value)
{
}

DelBlockRange::~DelBlockRange()
{
  count_ = 0;
}

void DelBlockRange::dump(FILE* fp) const
{
  if (max_value_ != -1)
  {
    fprintf(fp, "%d%% ~ %d%%: %"PRI64_PREFIX"d\n", min_value_, max_value_, count_);
  }
  else
  {
    fprintf(fp, "%d%% ~ : %"PRI64_PREFIX"d\n", min_value_, count_);
  }
}

BlockBase::BlockBase()
{
  memset(&info_, 0, sizeof(info_));
  server_list_.clear();
}
BlockBase::~BlockBase()
{
}
int32_t BlockBase::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t)
{
  if (input.getDataLen() <= 0 || offset >= length)
  {
    return TFS_ERROR;
  }
  int32_t len = input.getDataLen();
  input.readBytes(&info_, sizeof(info_));

  int8_t server_size = input.readInt8();
  while (server_size > 0)
  {
    ServerInfo server_info;
    server_info.server_id_ = input.readInt64();
    server_list_.push_back(server_info);
    server_size--;
  }
  offset += (len - input.getDataLen());
  return TFS_SUCCESS;
}

void BlockBase::dump() const
{
  TBSYS_LOG(INFO, "block_id: %u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %u, copys: %Zd",
      info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, info_.seq_no_, server_list_.size());
}

// stat info stat
StatInfo::StatInfo()
  : block_count_(0), file_count_(0), file_size_(0), del_file_count_(0), del_file_size_(0)
{
}
StatInfo::~StatInfo()
{
}
float StatInfo::div(const int64_t a, const int64_t b)
{
  float ret = 0;
  if (b > 0)
  {
    ret = static_cast<float>(a) / b;
  }
  return ret;
}

void StatInfo::add(const BlockBase& block_base)
{
  block_count_++;
  file_count_ += block_base.info_.file_count_;
  file_size_ += block_base.info_.size_;
  del_file_count_ += block_base.info_.del_file_count_;
  del_file_size_ += block_base.info_.del_size_;
}

void StatInfo::dump(FILE* fp) const
{
  fprintf(fp, "file_count: %"PRI64_PREFIX"d, file_size: %"PRI64_PREFIX"d, avg_file_size: %.2f, "
      "del_file_count: %"PRI64_PREFIX"d, del_file_size: %"PRI64_PREFIX"d, del_avg_file_size: %.2f, del_ratio: %.2f%%\n",
      file_count_, file_size_, div(file_size_, file_count_),
      del_file_count_, del_file_size_, div(del_file_size_, del_file_count_), div(del_file_size_ * 100, file_size_));
  fprintf(fp, "block_count: %d, avg_block_size: %.2f\n", block_count_, div(file_size_, block_count_));
}
