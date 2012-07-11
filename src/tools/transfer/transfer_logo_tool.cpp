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
*   daoan <daoan@taobao.com>
*      - initial release
*   duanfei <duanfei@taobao.com>
*      -modify-2011/12/29
*/

#include <tbsys.h>
#include <unistd.h>
#include "new_client/tfs_client_api.h"
#include "new_client/tfs_meta_client_api.h"
#include "new_client/tfs_rc_client_api.h"

#define buff_size  10 * 1024L * 1024L
#define TFS_MIN_FILE_LEN  18

using namespace tfs::common;
tfs::client::RcClient *rc_client = NULL;
static int64_t app_id = 0;
static int64_t hash_count = 0;
//iput_file
//source_file;/dest_file

enum ProcStage
{
  PARSE_LINE,
  OPEN_SOURCE,
  READ_SOURCE,
  SAVE_DEST
};

ProcStage g_stage = PARSE_LINE;

static int64_t get_source(const char* source_name, char* buff)
{
  int64_t source_count  = -1;
  if (*source_name == '/')
  {
    //get data from local file
    g_stage = OPEN_SOURCE;
    int fd = ::open(source_name, O_RDONLY);
    if (fd < 0)
    {
      fprintf(stderr, "open local file %s error, errno = %d\n", source_name, errno);
    }
    else
    {
      g_stage = READ_SOURCE;
      source_count = ::read(fd, buff, buff_size);
      if (source_count < 0 || source_count >= buff_size)
      {
        fprintf(stderr, "read file %s error %ld, errno = %d\n", source_name, source_count, errno);
        source_count = -1;
      }
      ::close(fd);
    }
  }
  else if(strlen(source_name) >= TFS_MIN_FILE_LEN &&
        (*source_name == 'T' || *source_name == 'L') &&
        isdigit(*(source_name + 1)))
  {
    g_stage = OPEN_SOURCE;
    //get data from tfs
    int fd = rc_client->open(source_name, NULL, tfs::client::RcClient::READ);
    if (fd < 0)
    {
      fprintf(stderr, "open tfs file %s error\n", source_name);
    }
    else
    {
      g_stage = READ_SOURCE;
      source_count = rc_client->read(fd, buff, buff_size);
      if (source_count < 0 || source_count > buff_size)
      {
        fprintf(stderr, "read file %s error %ld\n", source_name, source_count);
        source_count = -1;
      }
      rc_client->close(fd);
    }
  }
  else
  {
    fprintf(stderr, "unknow_source %s\n", source_name);
    source_count = -1;
  }
  return source_count;
}

static int64_t write_dest(const char* dest_name, char* buff, const int64_t size)
{
  g_stage = SAVE_DEST;
  int64_t write_count = -1;
  uint32_t  hash_value = tbsys::CStringUtil::murMurHash((const void*)(dest_name), strlen(dest_name));
  int32_t   uid = (hash_value % hash_count + 1);
  write_count = rc_client->save_buf(uid, buff, size, dest_name);
  if (write_count >= 0 || write_count == EXIT_TARGET_EXIST_ERROR) // exist or success
  {
    fprintf(stderr, "%s ok\n", dest_name);
  }
  else
  {
    fprintf(stderr, "%s error %"PRI64_PREFIX"d\n", dest_name, write_count);
  }
  return write_count;
}

int main(int argc ,char* argv[])
{
  if (argc != 5)
  {
    fprintf(stderr, "usage %s input_text  rcaddr app_key user_count\n", argv[0]);
    return 0;
  }

  TBSYS_LOGGER.setLogLevel("debug");
  rc_client = new tfs::client::RcClient();
  int ret = rc_client->initialize(argv[2], argv[3], "10.246.123.3");
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "rc_client initialize error %s\n", argv[2]);
    return -1;
  }

  // rc_client.set_wait_timeout(10000);

  FILE* fp = ::fopen(argv[1], "r");
  if (NULL == fp)
  {
    fprintf(stderr, "open local file %s error\n", argv[1]);
    return -1;
  }

  // global info for request
  app_id = rc_client->get_app_id();
  hash_count = atoll(argv[4]);
  char* buff = (char*)::malloc(buff_size);
  if (buff == NULL)
  {
    fprintf(stderr, "alloc memory fail\n");
    return -1;
  }
  char line_buff[4096];
  char* p_source = NULL;
  char* p_dest = NULL;
  while(fgets(line_buff, 4096, fp)!= NULL)
  {
    g_stage = PARSE_LINE;
    // TBSYS_LOG(WARN, "deal %s", line_buff);
    p_source = line_buff;
    p_dest = strstr(line_buff, ":");
    if (NULL != p_dest)
    {
      *(p_dest++) = '\0';
    }
    else
    {
      printf("parse source:dest error %s\n", line_buff);
      continue;
    }

    size_t count = strlen(p_dest);
    char *end = p_dest + count - 1;
    while (*end == '\n' || *end == '\r' || *end ==' ')
    {
      *end = 0;
      end--;
    }

    if (0 == strlen(p_source) || 0 == strlen(p_dest))
    {
      printf("parse source:dest error %s\n", line_buff);
      continue;
    }

    int64_t source_count = get_source(p_source, buff);
    int64_t dest_count = -1;
    if (source_count < 0)
    {
      printf("%s:%s fail read, stage=%d source=%"PRI64_PREFIX"d\n", p_source, p_dest, g_stage, source_count);
    }
    else
    {
      dest_count = write_dest(p_dest, buff, source_count);
      if (dest_count < 0)
      {
        printf("%s:%s fail all, stage=%d source=%"PRI64_PREFIX"d dest=%"PRI64_PREFIX"d\n",
            p_source, p_dest, g_stage, source_count, dest_count);
      }
      else if(source_count != dest_count)
      {
        printf("%s:%s fail partial, stage=%d source=%"PRI64_PREFIX"d dest=%"PRI64_PREFIX"d\n",
            p_source, p_dest, g_stage, source_count, dest_count);
      }
      else
      {
        printf("%s:%s transfer ok\n", p_source, p_dest);
      }
    }
  }

  ::fclose(fp);
  free (buff);

  delete rc_client;

  return 0;
}
