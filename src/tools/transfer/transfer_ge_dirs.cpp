/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   duanfei<duanfei@taobao.com>
*      - initial release
*   linqing <linqing.zyd@taobao.com>
*      - modify-2012/02/21
*
*/
#include <tbsys.h>
#include <unistd.h>
#include <string.h>
#include <set>
#include <errno.h>

#include "new_client/tfs_meta_client_api.h"

static FILE* input = NULL;
std::set<std::string> sets;
static tfs::client::NameMetaClient meta_client;
static int64_t app_id = 0;
static int64_t user_count = 0;

static int store_helper(const char* pstr)
{
  int32_t iret = NULL == pstr ? -1 : 0;
  if (0 == iret)
  {
    std::pair<std::set<std::string>::iterator, bool> res = sets.insert(std::string(pstr));
    iret = res.second ? 0 : -1;
    if (0 == iret)
    {
      printf("add string: %s\n", pstr);
    }
  }
  return iret;
}

static int store(char* pstart, char* pend)
{
  int32_t iret = NULL != pstart && NULL != pend  && pend != pstart ? 0 : -1;
  if (0 == iret)
  {
    char* p = strrchr(pstart, '/');
    if(NULL != p && p != pstart)
    {
      *p = '\0';
      store_helper(pstart);
    }
  }
  return iret;
}

static int open_file(const char* input_filepath)
{
  int32_t iret = NULL != input_filepath ? 0 : -1;
  if (0 == iret)
  {
    input = ::fopen(input_filepath, "r");
    iret = NULL != input ? 0 : -1;
    if (0 != iret)
    {
      TBSYS_LOG(ERROR, "open %s failed: %s", input_filepath, strerror(errno));
    }
  }
  return iret;
}

static void close_file(void)
{
  if (NULL != input)
    ::fclose(input);
}

int main(int argc ,char* argv[])
{
  if (argc != 5)
  {
    printf("usage %s input_text rsaddr app_id usercount\n", argv[0]);
    return 0;
  }
  int iret = meta_client.initialize(argv[2]);
  if (tfs::common::TFS_SUCCESS != iret)
  {
    TBSYS_LOG(ERROR, "initialize meta client failed, ret: %d", iret);
    return 0;
  }
  user_count = atoll(argv[4]);
  app_id     = atoll(argv[3]);

  TBSYS_LOG(INFO, "input_file:%s rootserver addr: %s app_id: %s, user_count: %s", argv[1], argv[2], argv[3], argv[4]);

  iret = open_file(argv[1]);
  if (0 == iret)
  {
    int64_t len = 0;
    const int32_t BUF_SIZE = 4096;
    char buf[BUF_SIZE];
    char* pend = NULL;
    char* pstart = NULL;
    while(fgets(buf, BUF_SIZE, input)!= NULL)
    {
      pstart = buf;
      while (*pstart != '/')
      {
        pstart++;
      }
      len = strlen(pstart);
      pend  = buf + len - 1;
      while (*pend == '\n' || *pend == '\r' || *pend == ' ')
      {
        *pend = '\0';
        pend--;
      }
      store(pstart, pend);
    }
    close_file();

    int ret = -1;
    int64_t total_error = 0;
    int64_t this_error = 0;
    int64_t i = 0;
    std::set<std::string>::const_iterator iter = sets.begin();
    for (; iter != sets.end(); ++iter)
    {
      this_error = 0;
      for (i = 1; i <= user_count; i++)
      {
        ret = meta_client.create_dir_with_parents(app_id, i, (*iter).c_str());
        if (ret != tfs::common::TFS_SUCCESS &&
            ret != tfs::common::EXIT_TARGET_EXIST_ERROR)
        {
          this_error++;
        }

        // log every 1000 users
        if(i % 1000 == 0 || i == user_count)
        {
          if(0 == this_error)
          {
            printf("%"PRI64_PREFIX"d%s:%s:%d\n", i, (*iter).c_str(), "succeed", ret);
          }
          else
          {
            printf("%"PRI64_PREFIX"d%s:%s:%d\n", i, (*iter).c_str(), "failed", ret);
          }
          total_error += this_error;
          this_error = 0;
        }
      }
     }
     printf("total errors: %"PRI64_PREFIX"d\n", total_error);
  }

  return 0;
}
