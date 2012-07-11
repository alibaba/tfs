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

void print_servers(const std::vector<uint64_t>& servers, std::string& result)
{
  std::vector<uint64_t>::const_iterator iter = servers.begin();
  for (; iter != servers.end(); ++iter)
  {
    result += "/";
    result += tbsys::CNetUtil::addrToString((*iter));
  }
}

int remove_block(const string& ns_ip, const string& file_path, const string& out_file_path)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen(file_path.c_str(), "r");
  if (NULL == fp)
  {
    TBSYS_LOG(ERROR, "open %s failed, %s %d", file_path.c_str(), strerror(errno), errno);
  }
  else
  {
    FILE* fp_out = fopen(out_file_path.c_str(), "w+");
    if (NULL == fp_out)
    {
      TBSYS_LOG(ERROR, "open %s failed, %s %d", out_file_path.c_str(), strerror(errno), errno);
    }
    else
    {
      uint64_t ns_ip_port = Func::get_host_ip(ns_ip.c_str());
      uint32_t block_id = 0;
      while (fscanf(fp, "%u\n", &block_id) != EOF)
      {
        int count = 0;
        sleep(2);
        do
        {
          count++;
          tbnet::Packet* ret_msg= NULL;
          ClientCmdMessage ns_req_msg;
          ns_req_msg.set_cmd(CLIENT_CMD_EXPBLK);
          ns_req_msg.set_value3(block_id);
          ns_req_msg.set_value4(1);
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s", block_id, ns_ip.c_str());
          }
          else
          {
            ret = send_msg_to_server(ns_ip_port, client, &ns_req_msg, ret_msg);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s", block_id, ns_ip.c_str());
            }
            else
            {
              ret = ret_msg->getPCode() == STATUS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s, response pcode: %d", block_id, ns_ip.c_str(), ret_msg->getPCode());
              }
              else
              {
                StatusMessage* msg = dynamic_cast<StatusMessage*>(ret_msg);
                ret = msg->get_status() == TFS_SUCCESS || msg->get_status() == EXIT_NO_BLOCK ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS != ret)
                  TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s, response pcode: %d, status: %d", block_id, ns_ip.c_str(), ret_msg->getPCode(), msg->get_status());
              }
            }
            NewClientManager::get_instance().destroy_client(client);
          }
        }
        while (count < 2 && TFS_SUCCESS == ret);

        if (TFS_SUCCESS == ret)
        {
          fprintf(fp_out, "%u\n", block_id);
        }
      }
      fclose(fp_out);
    }
    fclose(fp);
  }
  return ret;
}

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s nameserver ip -f file paths -o out file paths -l loglevel\n", name);
  fprintf(stderr, "       -d nameserver ip\n");
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

  string file_path;
  string dest_ns_ip;
  string out_file_path;
  string log_level("error");
  int i ;
  while ((i = getopt(argc, argv, "o:d:f:l:h")) != EOF)
  {
    switch (i)
    {
      case 'o':
        out_file_path = optarg;
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'd':
        dest_ns_ip = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  iret = !out_file_path.empty() && !file_path.empty() && dest_ns_ip.c_str() ? TFS_SUCCESS : TFS_ERROR;
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
    MessageFactory* factory = new MessageFactory();
    BasePacketStreamer* streamer = new BasePacketStreamer(factory);
    NewClientManager::get_instance().initialize(factory, streamer);
    iret = remove_block(dest_ns_ip, file_path, out_file_path);
    NewClientManager::get_instance().destroy();
    tbsys::gDelete(factory);
    tbsys::gDelete(streamer);
  }
  return iret;
}
