#include <set>
#include <map>
#include <string>
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "common/status_message.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

static const int32_t STEP = 10 * 1024 *1024;
static const int32_t RANGE = 9;
static tfs::common::BasePacketStreamer gstreamer;
static tfs::message::MessageFactory gfactory;

// block base construct
struct ServerInfo
{
  uint64_t server_id_;
  operator uint64_t() const {return server_id_;}
  ServerInfo& operator=(const ServerInfo& a)
  {
    server_id_ = a.server_id_;
    return *this;
  }
  bool operator==(const ServerInfo& b) const
  {
    return server_id_ == b.server_id_;
  }
  bool operator<<(std::ostream& os) const
  {
    return os << server_id_;
  }
};
class BlockBase
{
  public:
    BlockInfo info_;
    std::vector<ServerInfo> server_list_;

    BlockBase();
    virtual ~BlockBase();
    bool operator<(const BlockBase& b) const
    {
      return info_.block_id_ < b.info_.block_id_;
    }

    int32_t deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t type);
    void dump() const;
};

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
class StatInfo
{
  public:
    int32_t block_count_;
    int64_t file_count_;
    int64_t file_size_;
    int64_t del_file_count_;
    int64_t del_file_size_;
    StatInfo()
      : block_count_(0), file_count_(0), file_size_(0), del_file_count_(0), del_file_size_(0)
    {
    }
    ~StatInfo()
    {
    }

    static float div(const int64_t a, const int64_t b)
    {
      float ret = 0;
      if (b > 0)
      {
        ret = static_cast<float>(a) / b;
      }
      return ret;
    }

    void add(const BlockBase& block_base)
    {
      block_count_++;
      file_count_ += block_base.info_.file_count_;
      file_size_ += block_base.info_.size_;
      del_file_count_ += block_base.info_.del_file_count_;
      del_file_size_ += block_base.info_.del_size_;
    }

    void dump(FILE* fp) const
    {
      fprintf(fp, "file_count: %"PRI64_PREFIX"d, file_size: %"PRI64_PREFIX"d, avg_file_size: %.2f, "
          "del_file_count: %"PRI64_PREFIX"d, del_file_size: %"PRI64_PREFIX"d, del_avg_file_size: %.2f, del_ratio: %.2f%%\n",
          file_count_, file_size_, div(file_size_, file_count_),
          del_file_count_, del_file_size_, div(del_file_size_, del_file_count_), div(del_file_size_ * 100, file_size_));
      fprintf(fp, "block_count: %d, avg_block_size: %.2f\n", block_count_, div(file_size_, block_count_));
    }

};

class BlockSize
{
  public:
    BlockSize(const uint32_t block_id, const int32_t file_size)
      : block_id_(block_id), file_size_(file_size)
    {
    }
    ~BlockSize()
    {
    }

    uint32_t block_id_;
    int32_t file_size_;
    bool operator<(const BlockSize& b) const
    {
      if (file_size_ == b.file_size_)
      {
        return block_id_ < b.block_id_;
      }
      return file_size_ < b.file_size_;
    }
};

typedef set<BlockSize> BLOCK_SIZE_SET;
typedef set<BlockSize>::iterator BLOCK_SIZE_SET_ITER;
typedef map<int32_t, int64_t> BLOCK_COUNT_MAP;
typedef map<int32_t, int64_t>::iterator BLOCK_COUNT_MAP_ITER;

static inline uint64_t get_addr(const std::string& ns_ip_port)
{
  string::size_type pos = ns_ip_port.find_first_of(":");
  if (pos == string::npos)
  {
    return TFS_ERROR;
  }
  string tmp = ns_ip_port.substr(0, pos);
  return Func::str_to_addr(tmp.c_str(), atoi(ns_ip_port.substr(pos + 1).c_str()));
}

int show_block(const uint64_t ns_id, StatInfo& file_stat_info,
    BLOCK_COUNT_MAP& m_block_range_count, BLOCK_SIZE_SET& s_big_block, const int32_t top_num, BLOCK_SIZE_SET& s_topn_block)
{
  const int32_t num = 1000;

  ShowServerInformationMessage msg;
  SSMScanParameter& param = msg.get_param();
  param.type_ = SSM_TYPE_BLOCK;
  param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;
  param.should_actual_count_ = (num << 16);
  param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

  while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
  {
    param.data_.clear();
    tbnet::Packet*ret_msg = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    int ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
    if (TFS_SUCCESS != ret || ret_msg == NULL)
    {
      TBSYS_LOG(ERROR, "get block info error, ret: %d", ret);
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }
    //TBSYS_LOG(DEBUG, "pCode: %d", ret_msg->get_message_type());
    if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
    {
      if (ret_msg->getPCode() == STATUS_MESSAGE)
      {
        StatusMessage* msg = dynamic_cast<StatusMessage*>(ret_msg);
        TBSYS_LOG(ERROR, "get invalid message type: error: %s", msg->get_error());
      }
      TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }
    ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
    SSMScanParameter& ret_param = message->get_param();

    int32_t data_len = ret_param.data_.getDataLen();
    int32_t offset = 0;
    while (data_len > offset)
    {
      BlockBase block;
      if (TFS_SUCCESS == block.deserialize(ret_param.data_, data_len, offset, param.child_type_))
      {
        //block.dump();
        file_stat_info.add(block);
        int32_t index = block.info_.size_ / STEP;
        if (index > RANGE - 1) index = RANGE - 1;
        BLOCK_COUNT_MAP_ITER iter;
        iter = m_block_range_count.find(index);
        if (iter == m_block_range_count.end())
        {
          TBSYS_LOG(ERROR, "some error occurs, size: %d", block.info_.size_);
        }
        else
        {
          iter->second++;
          if (index == RANGE - 1)
          {
            s_big_block.insert(BlockSize(block.info_.block_id_, block.info_.size_));
          }
        }
        if (static_cast<int32_t>(s_topn_block.size()) >= top_num)
        {
          if (block.info_.size_ > s_topn_block.begin()->file_size_)
          {
            s_topn_block.erase(s_topn_block.begin());
            s_topn_block.insert(BlockSize(block.info_.block_id_, block.info_.size_));
          }
        }
        else
        {
          s_topn_block.insert(BlockSize(block.info_.block_id_, block.info_.size_));
        }
      }
    }
    param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
    param.end_flag_ = ret_param.end_flag_;
    if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
    {
      param.addition_param1_ = ret_param.addition_param2_;
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return TFS_SUCCESS;
}

void usage(const char *name)
{
  fprintf(stderr, "\n****************************************************************************** \n");
  fprintf(stderr, "use this tool to get cluster file and block stat info.\n");
  fprintf(stderr, "Usage: \n");
  fprintf(stderr, "  %s -s ns_addr [-f log_file] [-m top_num] [-n]\n", name);
  fprintf(stderr, "     -s ns_addr.   cluster ns addr\n");
  fprintf(stderr, "     -f log_file.  log file for recoding stat info, default is stat_log\n");
  fprintf(stderr, "     -m top_num.   one stat info param, to get top n most biggest blocks, default is 20\n");
  fprintf(stderr, "     -n.           set log level to info, default is debug\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "\n");
  exit(TFS_ERROR);
}


int main(int argc,char** argv)
{
  int32_t i;
  string ns_addr;
  string log_file("stat_log");
  int32_t top_num = 20;
  bool is_debug = true;
  while ((i = getopt(argc, argv, "s:f:m:nh")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_addr = optarg;
        break;
      case 'f':
        log_file = optarg;
        break;
      case 'm':
        top_num = atoi(optarg);
        break;
      case 'n':
        is_debug = false;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (ns_addr.empty())
  {
    fprintf(stderr, "please input nameserver ip and port.\n");
    usage(argv[0]);
  }
  uint64_t ns_id = get_addr(ns_addr);
  FILE* fp = fopen(log_file.c_str(), "w");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open log file failed. file: %s", log_file.c_str());
    return TFS_ERROR;
  }

  if (! is_debug)
  {
    TBSYS_LOGGER.setLogLevel("info");
  }

  gstreamer.set_packet_factory(&gfactory);
  NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  StatInfo stat_info;
  map<int32_t, int64_t > m_block_range_count;
  set<BlockSize> s_big_block;
  set<BlockSize> s_topn_block;

  int32_t index = 0;
  for (index = 0; index < RANGE; index++)
  {
    m_block_range_count[index] = 0;
  }

  show_block(ns_id, stat_info, m_block_range_count, s_big_block, top_num, s_topn_block);
  fprintf(fp, "--------------------------block file info-------------------------------\n");
  stat_info.dump(fp);
  fprintf(fp, "--------------------------block size range-------------------------------\n");
  for (index = 0; index < RANGE; index++)
  {
    if (index < (RANGE - 1))
    {
      fprintf(fp, "%d ~ %d: %"PRI64_PREFIX"d\n", index * STEP, (index + 1) * STEP, m_block_range_count[index]);
    }
    else
    {
      fprintf(fp, "%d ~ : %"PRI64_PREFIX"d\n", index * STEP,  m_block_range_count[index]);
    }
  }
  fprintf(fp, "--------------------------block list whose size bigger than 8M. num: %zd -------------------------------\n", s_big_block.size());
  set<BlockSize>::reverse_iterator rbiter = s_big_block.rbegin();
  for (; rbiter != s_big_block.rend(); rbiter++)
  {
    fprintf(fp, "block_id: %u, size: %d\n", rbiter->block_id_, rbiter->file_size_);
  }
  fprintf(fp, "--------------------------top %d block list-------------------------------\n", top_num);
  set<BlockSize>::reverse_iterator rtiter = s_topn_block.rbegin();
  for (; rtiter != s_topn_block.rend(); rtiter++)
  {
    fprintf(fp, "block_id: %u, size: %d\n", rtiter->block_id_, rtiter->file_size_);
  }
  fclose(fp);
}
