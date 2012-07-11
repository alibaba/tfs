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
#include "new_client/tfs_rc_client_api.h"
#include "common/internal.h"
#include "common/directory_op.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "common/directory_op.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace tfs::tools;

static RcClient* gclient;
int verify_file(const string& file_path, const string& out_file_path)
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
    FILE* out_fp = fopen(out_file_path.c_str(), "w+");
    if (NULL == out_fp)
    {
      TBSYS_LOG(ERROR, "open %s failed, %s %d", out_file_path.c_str(), strerror(errno), errno);
    }
    else
    {
      char record[64];
      char filename[64];
      int32_t fd = -1;
      int64_t ret_nums = 0;
      int64_t len = 0, total = 0, ret = -1;
      const int32_t MAX_BUF_LEN = 2 * 1024 * 1024;
      char buf[MAX_BUF_LEN];
      TfsFileStat stat;
      while (NULL != fgets(record, 64, fp))
      {
        uint32_t crc = 0;
        memset(&stat, 0, sizeof(stat));
        ret_nums = sscanf(record, "%s", filename);
        ret = 1 != ret_nums ? -1 : 0;
        if (0 != ret)
        {
          TBSYS_LOG(ERROR, "sscanf error, record: %s, nums %"PRI64_PREFIX"d <> 1", record, ret_nums);
        }
        else
        {
          fd = gclient->open(filename, NULL, RcClient::READ, true);
          ret = fd < 0 ? -1 : 0;
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "open %s failed, ret: %"PRI64_PREFIX"d", filename, ret);
          }
          else
          {
            ret = gclient->fstat(fd, &stat);
            if (ret < 0)
            {
              TBSYS_LOG(ERROR, "stat %s failed, ret:%"PRI64_PREFIX"d", filename, ret);
            }
            else
            {
              ret = 0;
              while (total < stat.size_)
              {
                len = gclient->readv2(fd, buf, MAX_BUF_LEN, &stat);
                if (len > 0)
                {
                  total += len;
                  crc = Func::crc(crc, buf, len);
                }
                else if (len < 0)
                {
                  ret = -1;
                  TBSYS_LOG(ERROR, "read data failed from %s, ret: %ld", filename, len);
                  break;
                }
              }
            }
            gclient->close(fd);
          }
        }
        if (0 != ret)
        {
          fprintf(out_fp, "%s", filename);
        }
        else
        {
          if (stat.size_ != total
            || stat.crc_ != crc)
          {
            fprintf(out_fp, "%s", filename);
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
  fprintf(stderr, "Usage: %s -s resource server ip -f file paths -o out file paths -a app_key -l loglevel\n", name);
  fprintf(stderr, "       -s resource server ip\n");
  fprintf(stderr, "       -a app key\n");
  fprintf(stderr, "       -f file path\n");
  fprintf(stderr, "       -o out file path\n");
  fprintf(stderr, "       -l log level\n");
  fprintf(stderr, "       -h help\n");
  exit(0);
}

int main(int argc, char** argv)
{
  int iret = TFS_ERROR;
  if(argc < 5)
  {
    usage(argv[0]);
  }

  string rcs_ip;
  string file_path;
  string app_key;
  string out_file_path;
  string log_level("error");
  int i ;
  while ((i = getopt(argc, argv, "s:f:o:l:a:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        rcs_ip = optarg;
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
      case 'a':
        app_key = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  iret = !rcs_ip.empty() && !file_path.empty() && !app_key.empty() && !out_file_path.empty() ? TFS_SUCCESS : TFS_ERROR;
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
    gclient = new RcClient();
    gclient->initialize(rcs_ip.c_str(), app_key.c_str());
    iret = verify_file(file_path, out_file_path);
    tbsys::gDelete(gclient);
  }
  return iret;
}
