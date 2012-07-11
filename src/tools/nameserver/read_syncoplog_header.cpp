/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   mingyan.zc <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/func.h"
#include "common/file_queue.h"
#include "nameserver/oplog.h"

using namespace std;
using namespace tfs;
using namespace nameserver;

bool gstop = false;
static const std::string rotateheader("rotateheader.dat");
static const std::string header("header.dat");

void signlHandler(int signal)
{
  switch(signal)
  {
    case SIGINT:
      gstop = true;
      break;
    default:
      fprintf(stderr, "[INFO]: occur signl(%d)", signal);
      break;
  }
}

int read_header(std::string& dir_name)
{
  OpLogRotateHeader header;
  ::memset(&header, 0, sizeof(header));
  std::string headPath = dir_name + "/rotateheader.dat";
  int fd = open(headPath.c_str(), O_RDONLY, 0600);
  if (fd < 0)
  {
    fprintf(stderr, "[ERROR]: open file(%s) failed(%s)\n", headPath.c_str(), strerror(errno));
    return EXIT_FAILURE;
  }
  int iret = read(fd, &header, sizeof(header));
  if (iret != sizeof(header))
  {
    fprintf(stderr, "[ERROR]: read file(%s) failed\n", headPath.c_str());
    ::close(fd);
    return EXIT_FAILURE;
  }
  ::close(fd);
  std::string fileQueueHeadPath = dir_name + "/header.dat";
  fd = open(fileQueueHeadPath.c_str(), O_RDWR, 0600);
  if (fd < 0)
  {
    fprintf(stderr, "[ERROR]: open file(%s) failed(%s)\n", fileQueueHeadPath.c_str(), strerror(errno));
    return EXIT_FAILURE;
  }
  common::QueueInformationHeader qhead;
  iret = read(fd, &qhead, sizeof(qhead));
  if (iret != sizeof(qhead))
  {
    fprintf(stderr, "[ERROR]: read file(%s) failed\n", fileQueueHeadPath.c_str());
    ::close(fd);
    return EXIT_FAILURE;
  }
  ::close(fd);

  TBSYS_LOG(DEBUG, "current file queue header: read seqno: %d, read offset: %d, write seqno: %d,"
    "write file size: %d, queue size: %d. oplog header:, rotate seqno: %d, "
    "rotate offset: %d", qhead.read_seqno_, qhead.read_offset_, qhead.write_seqno_, qhead.write_filesize_,
    qhead.queue_size_, header.rotate_seqno_, header.rotate_offset_);

  return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
  signal(SIGINT, signlHandler);
  std::string dir_name;
  int32_t i = 0;
  while ((i = getopt(argc, argv, "f:h")) != EOF)
  {
    switch (i)
    {
      case 'f':
        dir_name = optarg;
        break;
      case 'h':
      default:
        fprintf(stderr, "Usage: %s -f fileQueueDirPath\n", argv[0]);
        fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
        fprintf(stderr, "       -h help\n");
        return EXIT_SUCCESS;
    }
  }
  if (dir_name.empty())
  {
    fprintf(stderr, "Usage: %s -f fileQueueDirPath\n", argv[0]);
    fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
    fprintf(stderr, "       -h help\n");
    return EXIT_SUCCESS;
  }

  struct stat statbuf;
  if (stat(dir_name.c_str(), &statbuf) != 0)
  {
    fprintf(stderr, "%s:%d [ERROR]: (%s%s)\n", __FILE__, __LINE__, dir_name.c_str(), strerror(errno));
    return EXIT_SUCCESS;
  }
  if (!S_ISDIR(statbuf.st_mode))
  {
    fprintf(stderr, "%s:%d [ERROR]: (%s) not directory\n", __FILE__, __LINE__, dir_name.c_str());
    return EXIT_SUCCESS;
  }

  int iret = read_header(dir_name);
  if (iret != EXIT_SUCCESS)
  {
    fprintf(stderr, "%s:%d [ERROR]: do work error\n", __FILE__, __LINE__);
  }
  return EXIT_SUCCESS;
}

