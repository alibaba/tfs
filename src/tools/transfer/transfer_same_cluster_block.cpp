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

#include "common/func.h"
#include "new_client/fsname.h"
#include "common/internal.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "common/directory_op.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/read_data_message.h"
#include "message/message_factory.h"
#include "new_client/tfs_client_api.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;
using namespace tfs::client;

struct ReadDataStruct
{
  FileInfo info;
  char* buf;
  int32_t  offset;
  int32_t  buf_len;
  int32_t  size;
  int32_t  flag;
};

struct Record
{
  char name[32];
  char src [32];
  char dest[32];
  int32_t usize;
  int32_t size;
  int32_t flag;
};

static int read_data(const Record& rd, ReadDataStruct& info)
{
  int ret = info.size <= 0 ? -1 : 0;
  if (TFS_SUCCESS == ret)
  {
    FSName name(rd.name);
    ReadDataMessageV2 req_msg;
    req_msg.set_block_id(name.get_block_id());
    req_msg.set_file_id(name.get_file_id());
    req_msg.set_offset(info.offset);
    req_msg.set_length(info.size);
    req_msg.set_flag(info.flag);
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = NULL != client ? 0: -1;
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "create new client error");
    }
    if (0 == ret)
    {
      tbnet::Packet* ret_msg= NULL;
      uint64_t id = Func::get_host_ip(rd.src);
      ret = send_msg_to_server(id, client, &req_msg, ret_msg) != TFS_SUCCESS ? -1 : 0;
      if (0 != ret)
      {
        TBSYS_LOG(ERROR, "send msg to server:%s failed, ret: %d", rd.src, ret);
      }
      else
      {
        ret = ret_msg->getPCode() == RESP_READ_DATA_MESSAGE_V2 ? 0: -1;
        if (0 == ret)
        {
          RespReadDataMessageV2* msg = dynamic_cast<RespReadDataMessageV2*>(ret_msg);
          if (NULL != msg->get_file_info())
            info.info = *msg->get_file_info();
          ret = std::min(msg->get_length(), info.buf_len - info.offset);
          if (ret > 0)
            memcpy((info.buf + info.offset), msg->get_data(), ret);
        }
      }
    }
  }
  return ret;
}

int transfer_file(const string& file_path)
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
    Record rd;
    ReadDataStruct info;
    memset(&info, 0, sizeof(info));
    while (fscanf(fp, "%s %s %s %d %d %d\n",rd.name,rd.src,rd.dest,&rd.usize, &rd.size, &rd.flag) != EOF)
    {
      rd.name[18]='\0';
      TBSYS_LOG(DEBUG, "============================%zd", strlen(rd.name));
      tbsys::gDelete(info.buf);
      info.buf = new char[rd.usize];
      info.buf_len = rd.usize;
      info.offset  = 0;
      info.size = rd.usize;
      info.flag    = READ_DATA_OPTION_FLAG_FORCE;
      int64_t len = 0;
      uint32_t crc = 0;
      while (info.offset < rd.usize)
      {
        len = read_data(rd, info);
        if (len <= 0)
          break;
        else
        {
          crc = Func::crc(crc, (info.buf + info.offset), len);
          info.offset += len;
        }
      }

      if (info.offset != rd.usize
         || info.info.crc_ != crc)
      {
        TBSYS_LOG(ERROR, "transfer file: %s failed, read data error %d <> %d or crc %u <> %u",
          rd.name, rd.usize, info.offset, info.info.crc_, crc);
      }
      else
      {
        int fd = TfsClient::Instance()->open(rd.name, NULL, T_WRITE|T_CREATE);
        if (fd <= 0)
        {
          TBSYS_LOG(ERROR, "transfer file: %s failed, open failed", rd.name);
        }
        else
        {
          len = TfsClient::Instance()->write(fd, info.buf, rd.usize);
          if (len != rd.usize)
          {
            TBSYS_LOG(ERROR, "transfer file: %s failed, write data failed %ld <> %d", rd.name, len, rd.usize);
          }
          else
          {
            TBSYS_LOG(INFO, "transfer file: %s successful, write data successful %ld == %d", rd.name, len, rd.usize);
          }
          TfsClient::Instance()->close(fd);
        }
      }
    }
    fclose(fp);
  }
  return ret;
}

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s nameserver ip -f file paths -l loglevel\n", name);
  fprintf(stderr, "       -s nameserver ip\n");
  fprintf(stderr, "       -f file path\n");
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
  string log_level("debug");
  int i ;
  while ((i = getopt(argc, argv, "s:f:l:h")) != EOF)
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
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  iret = !ns_ip.empty() && !file_path.empty() ? TFS_SUCCESS : TFS_ERROR;
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
    iret = TfsClient::Instance()->initialize(ns_ip.c_str());
    if (TFS_SUCCESS == iret)
    {
      iret = transfer_file(file_path);
    }
    TfsClient::Instance()->destroy();
  }
  return iret;
}
