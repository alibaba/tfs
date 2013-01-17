/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: stat_main.cpp 1737 2012-11-28 06:50:01Z chuyu $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "stat_tool.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

static tfs::message::MessageFactory gfactory;
static tfs::common::BasePacketStreamer gstreamer;

template <class T>
int32_t do_stat(const int32_t size, T& v_value_range)
{
  int32_t ret = TFS_ERROR;
  typename T::iterator iter = v_value_range.begin();
  for(; iter != v_value_range.end(); iter++)
  {
    if (iter->is_in_range(size))
    {
      iter->incr();
      ret = TFS_SUCCESS;
      break;
    }
  }
  return ret;
}

int block_process(const uint64_t ns_id, StatInfo& file_stat_info,
    V_BLOCK_SIZE_RANGE& v_block_size_range, V_DEL_BLOCK_RANGE& v_del_block_range, BLOCK_SIZE_SET& s_bigid_block, BLOCK_SIZE_SET& s_big_block, const int32_t top_num, BLOCK_SIZE_SET& s_topn_block)
{
  const int32_t num = 1000;
  int32_t ret  = TFS_ERROR;

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
    ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
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
        // add to file stat
        file_stat_info.add(block);

        // get block with big id
        if (block.info_.block_id_ >= MAGIC_BLOCK_ID)
        {
          s_bigid_block.insert(BlockSize(block.info_.block_id_, block.info_.size_));
        }

        // do range stat
        ret = do_stat(block.info_.size_ / M_UNITS, v_block_size_range);
        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "some error occurs, blockid: %u, size: %d", block.info_.block_id_, block.info_.size_);
        }

        // get big block info
        if (block.info_.size_ >= THRESHOLD)
        {
          s_big_block.insert(BlockSize(block.info_.block_id_, block.info_.size_));
        }

        // count del block range
        if (block.info_.del_size_ < 0)
        {
          TBSYS_LOG(ERROR, "some error occurs, blockid: %u, del size: %d, size: %d", block.info_.block_id_, block.info_.del_size_, block.info_.size_);
        }
        else
        {
          int32_t percent = block.info_.size_ > 0 ? static_cast<int32_t>(static_cast<int64_t>(block.info_.del_size_) * 100 / block.info_.size_) : 0;
          ret = do_stat(percent, v_del_block_range);
          if (ret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "some error occurs, blockid: %u, del size: %d, size: %d", block.info_.block_id_, block.info_.del_size_, block.info_.size_);
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
  V_BLOCK_SIZE_RANGE v_block_size_range;
  V_DEL_BLOCK_RANGE v_del_block_range;
  set<BlockSize> s_bigid_block;
  set<BlockSize> s_big_block;
  set<BlockSize> s_topn_block;

  // initialize
  int32_t row = 0;
  while (RANGE_ARR[row][0] != -1)
  {
    v_block_size_range.push_back(BlockSizeRange(RANGE_ARR[row][0], RANGE_ARR[row][1]));
    row++;
  }
  row = 0;
  while (DEL_RANGE_ARR[row][0] != -1)
  {
    v_del_block_range.push_back(DelBlockRange(DEL_RANGE_ARR[row][0], DEL_RANGE_ARR[row][1]));
    row++;
  }

  // do stat
  block_process(ns_id, stat_info, v_block_size_range, v_del_block_range, s_bigid_block, s_big_block, top_num, s_topn_block);

  // print info to log file
  fprintf(fp, "--------------------------block file info-------------------------------\n");
  stat_info.dump(fp);
  fprintf(fp, "--------------------------block size range-------------------------------\n");
  V_BLOCK_SIZE_RANGE_ITER siter = v_block_size_range.begin();
  for (; siter != v_block_size_range.end(); siter++)
  {
    siter->dump(fp);
  }
  fprintf(fp, "--------------------------del block range-------------------------------\n");
  V_DEL_BLOCK_RANGE_ITER diter = v_del_block_range.begin();
  for (; diter != v_del_block_range.end(); diter++)
  {
    diter->dump(fp);
  }
  fprintf(fp, "--------------------------big blockid list whose id is bigger than %d. num: %zd -------------------------------\n", MAGIC_BLOCK_ID, s_bigid_block.size());
  set<BlockSize>::iterator biditer = s_bigid_block.begin();
  for (; biditer != s_bigid_block.end(); biditer++)
  {
    fprintf(fp, "block_id: %u, size: %d\n", biditer->block_id_, biditer->file_size_);
  }
  fprintf(fp, "--------------------------block list whose size bigger is than %d. num: %zd -------------------------------\n", THRESHOLD, s_big_block.size());
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
