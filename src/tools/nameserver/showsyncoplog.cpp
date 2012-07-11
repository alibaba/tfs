/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: showsyncoplog.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
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

int print_information(std::string& dir_name, int type = 0x00)
{
  OpLogRotateHeader header;
  ::memset(&header, 0, sizeof(header));
  std::string headPath = dir_name + "/rotateheader.dat";
  int fd = open(headPath.c_str(), O_RDWR, 0600);
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
  fprintf(stderr, "--------------------------------------------------------------\n");
  fprintf(stderr, "        current rotate information           \n\n");
  fprintf(stderr, "RotateSeqno:%d  RotateOffset: %d \n\n",header.rotate_seqno_, header.rotate_offset_);
  ::close(fd);

  std::string fileQueuePath = dir_name + "/header.dat";
  int qfd = open(fileQueuePath.c_str(), O_RDWR, 0600);
  if (qfd < 0)
  {
    fprintf(stderr, "%s:%d [ERROR]: open file(%s) failed(%s)\n", __FILE__, __LINE__, fileQueuePath.c_str(), strerror(errno));
    return EXIT_FAILURE;
  }
  common::QueueInformationHeader qhead;
  ::memset(&qhead, 0, sizeof(qhead));
  if (read(qfd, &qhead, sizeof(qhead)) != sizeof(qhead))
  {
    fprintf(stderr, "%s:%d [ERROR]: read file(%s) failed\n", __FILE__, __LINE__,fileQueuePath.c_str());
    ::close(qfd);
    return EXIT_FAILURE;
  }
  fprintf(stderr, "        current synchronization oplog information          \n\n");
  fprintf(stderr, "ReadSeqno:  %d     ReadOffset: %d  \n", qhead.read_seqno_, qhead.read_offset_);
  fprintf(stderr, "WriteSeqno: %d     WriteFileSize: %d  \n", qhead.write_seqno_, qhead.write_filesize_);
  fprintf(stderr, "QueueSize: %d\n", qhead.queue_size_);
  if ( type == 0x00)
  {
    ostringstream current;
    for (int32_t i = 0; i < common::FILE_QUEUE_MAX_THREAD_SIZE; i++)
    {
      current << "     " << i << "              " << qhead.pos_[i].seqno_ <<"         "<< qhead.pos_[i].offset_ <<"\n";
    }
    fprintf(stderr, "Current Process: \n");
    fprintf(stderr, "Thread Number     SeqNo     Offset \n");
    fprintf(stderr, "%s\n", current.str().c_str());
  }
  ::close(qfd);
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
  signal(SIGINT, signlHandler);
  std::string dir_name;
  int32_t i = 0;
  int32_t count = 0;
  int32_t type = 0x00;
  int32_t interval = 0x01;
  while ((i = getopt(argc, argv, "f:i:c:t:h")) != EOF)
  {
    switch (i)
    {
      case 'f':
        dir_name = optarg;
        break;
      case 'c':
        count = atoi(optarg);
        break;
      case 'i':
        interval = atoi(optarg);
        break;
      case 't':
        type = atoi(optarg);
        break;
      case 'h':
      default:
        fprintf(stderr, "Usage: %s -f fileQueueDirPath -t type -c count -i interval\n", argv[0]);
        fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
        fprintf(stderr, "       -t type : 0: all 1: not print currnet process information\n");
        fprintf(stderr, "       -c count: count\n");
        fprintf(stderr, "       -i interval: interval time\n");
        fprintf(stderr, "       -h help\n");
        return EXIT_SUCCESS;
    }
  }
  if (dir_name.empty())
  {
    fprintf(stderr, "Usage: %s -f fileQueueDirPath -t type -c count -i interval\n", argv[0]);
    fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
    fprintf(stderr, "       -t type : 0: all 1: not print currnt process information\n");
    fprintf(stderr, "       -c count: count\n");
    fprintf(stderr, "       -i interval: interval time\n");
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

  int iret = 0;
  for (int i = 0; (i < count || count == 0); i++)
  {
    iret = print_information(dir_name, type);
    if ((iret == EXIT_FAILURE)
        || ((count == i + 1) && (count != 0))
        || (gstop))
      break;
    common::Func::sleep(interval, gstop);
  }
  return EXIT_SUCCESS;
}

