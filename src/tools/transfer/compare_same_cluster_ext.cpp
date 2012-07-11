/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */


#include "new_client/fsname.h"
#include "common/internal.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "common/directory_op.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

static bool compare(const FileInfo& src, const FileInfo& dest)
{
  return src.crc_ == dest.crc_
          && src.size_ == dest.size_;
}

int compare_block(const string& ns_ip, const string& file_path, const string& out_file_path)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen(file_path.c_str(), "r");
  if (NULL == fp)
  {
    TBSYS_LOG(ERROR, "open %s failed, %s %d", file_path.c_str(), strerror(errno), errno);
  }
  else
  {
    TBSYS_LOG(INFO,"open %s successful", file_path.c_str());
    FILE* out_fp = fopen(out_file_path.c_str(), "rw+");
    if (NULL == out_fp)
    {
      TBSYS_LOG(ERROR, "open %s failed, %s %d", out_file_path.c_str(), strerror(errno), errno);
    }
    else
    {
      uint32_t block_id = 0;
      while (fscanf(fp, "%u\n", &block_id) != EOF)
      {
        sleep(2);
        VUINT64 ds_list;
        ToolUtil::get_block_ds_list(Func::get_host_ip(ns_ip.c_str()), block_id, ds_list);
        if (ds_list.empty())
        {
          TBSYS_LOG(ERROR, "compacre block: %u failed, ds_list empty", block_id);
        }
        else if (1 == ds_list.size())
        {
          TBSYS_LOG(ERROR, "compacre block: %u failed, ds_list.size() == 1", block_id);
        }
        else
        {
          int32_t index = 0;
          NewClient* client = NULL;
          tbnet::Packet* ret_msg= NULL;
          FILE_INFO_LIST file_list;
          GetServerStatusMessage req_msg;
          req_msg.set_status_type(GSS_BLOCK_FILE_INFO);
          req_msg.set_return_row(block_id);
          const uint32_t size = ds_list.size();
          std::map<uint64_t, FileInfo> files[size];
          VUINT64::const_iterator iter = ds_list.begin();
          for (ret = TFS_SUCCESS; iter != ds_list.end() && TFS_SUCCESS == ret;
              ++iter, ++index)
          {
            client = NewClientManager::get_instance().create_client();
            ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "compacre block: %u failed", block_id);
            }
            else
            {
              ret_msg = NULL;
              TBSYS_LOG(INFO, "server ===> %s", tbsys::CNetUtil::addrToString((*iter)).c_str());
              ret = send_msg_to_server((*iter), client, &req_msg, ret_msg);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "compacre block: %u failed, ret: %d", block_id, ret);
              }
              else
              {
                ret = ret_msg->getPCode() == BLOCK_FILE_INFO_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "compacre block: %u failed", block_id);
                }
                else
                {
                  BlockFileInfoMessage *bfi_message = dynamic_cast<BlockFileInfoMessage*>(ret_msg);
                  FILE_INFO_LIST* file_info_list = bfi_message->get_fileinfo_list();
                  FILE_INFO_LIST::iterator vit = file_info_list->begin();
                  for (; vit != file_info_list->end();++vit)
                  {
                    files[index].insert(std::map<uint64_t, FileInfo>::value_type((*vit).id_, (*vit)));
                  }
                }
              }
              NewClientManager::get_instance().destroy_client(client);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            bool bfind = false;
            ret = size == 0U ? TFS_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              for (uint32_t i = 0; i < size; i++)
              {
                if (!files[i].empty())
                {
                  bfind = true;
                  break;
                }
              }
            }
            if (TFS_ERROR == ret
                || !bfind)
            {
              ret = TFS_ERROR;
              fprintf(out_fp, "%u\n", block_id);
              TBSYS_LOG(ERROR, "diff block: %u", block_id);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            bool diff = false;
            std::map<uint64_t, FileInfo>::iterator it;
            for (uint32_t i = 0; i < size && !diff; i++)
            {
              std::map<uint64_t, FileInfo>::iterator iter = files[i].begin();
              while (iter != files[i].end() && !diff)
              {
                const FileInfo& info = (iter->second);
                for (uint32_t index = 0; index < size; index++)
                {
                  if (index == i)
                    continue;
                  it = files[index].find(iter->first);
                  if (it == files[index].end())
                  {
                    diff = true;
                    break;
                  }
                  else
                  {
                    const FileInfo& tmp = (it->second);
                    if (!compare(info, tmp))
                    {
                      diff = true;
                      break;
                    }
                    files[index].erase(it++);
                  }
                }
                files[i].erase(iter++);
              }
            }

            if (diff)
            {
              fprintf(out_fp, "%u\n", block_id);
              TBSYS_LOG(ERROR, "diff block: %u", block_id);
            }
          }
        }
      }
      fclose(out_fp);
    }
    fclose(fp);
  }
  TBSYS_LOG(INFO, " process exit!!!!");
  return ret;
}

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s nameserver ip -f file paths -o out file paths -l loglevel\n", name);
  fprintf(stderr, "       -s nameserver ip\n");
  fprintf(stderr, "       -f file path\n");
  fprintf(stderr, "       -o out file path\n");
  fprintf(stderr, "       -l log level\n");
  fprintf(stderr, "       -h help\n");
  exit(0);
}

int main(int argc, char** argv)
{
  int iret = TFS_ERROR;
  if(argc < 3)
  {
    usage(argv[0]);
  }

  string ns_ip;
  string file_path;
  string out_file_path;
  string block_out_file_path;
  string log_level("error");
  int i ;
  while ((i = getopt(argc, argv, "s:f:o:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_ip = optarg;
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'o':
        out_file_path = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  iret = !ns_ip.empty() && !file_path.empty() && !out_file_path.empty() ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS != iret)
  {
    usage(argv[0]);
  }

  TBSYS_LOGGER.setLogLevel(log_level.c_str());

  iret = access(file_path.c_str(), R_OK|F_OK);
  if (0 != iret)
  {
    TBSYS_LOG(ERROR, "open %s failed, error: %s ", file_path.c_str(), strerror(errno));
  }
  else
  {
    iret = access(out_file_path.c_str(), R_OK|F_OK);
    if (0 != iret)
    {
      TBSYS_LOG(ERROR, "open %s failed, error: %s ", out_file_path.c_str(), strerror(errno));
    }
    else
    {
      MessageFactory* factory = new MessageFactory();
      BasePacketStreamer* streamer = new BasePacketStreamer(factory);
      NewClientManager::get_instance().initialize(factory, streamer);
      iret = compare_block(ns_ip, file_path, out_file_path);
      NewClientManager::get_instance().destroy();
      tbsys::gDelete(factory);
      tbsys::gDelete(streamer);
    }
  }
  return iret;
}
