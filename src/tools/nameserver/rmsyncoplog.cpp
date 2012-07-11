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
 *   duolong <duolong@taobao.com>
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

//scans the directory tree gets, starting from directory and adds the result into tree
//return number of files + ddirectories found
bool scan_directory_tree (std::string& directory, std::vector<std::string>& vec, uint64_t maxIndex)
{
  struct dirent dirent;
  struct dirent *result = NULL;
  DIR *dir = opendir (directory.c_str());
  if (!dir)
  {
    return false;
  }

  while (!readdir_r (dir, &dirent, &result) && result)
  {
    char *name = result->d_name;
    if (((name[0] == '.') && (name[1] == '\0'))
        || ((name[0] == '.') && (name[1] == '.') && (name[2] == '\0')))
    {
      continue;
    }
    std::string tmp(name);
    if (tmp.compare(rotateheader) == 0
        ||tmp.compare(header) == 0)
    {
      continue;
    }

    std::string::size_type pos = tmp.find(".");
    std::string number = tmp.substr(0, pos);
    if (!number.empty())
    {
      uint64_t seq = atoll(number.c_str());
      if (seq < maxIndex)
      {
        std::string parent_name = directory;
        parent_name += "/";
        parent_name += tmp;
        vec.push_back (parent_name);
      }
    }
  }
  closedir (dir);
  return true;
}

int do_unlink_file(std::string& dir_name)
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

  uint32_t seqno = std::min(header.rotate_seqno_, qhead.read_seqno_);
  std::vector<std::string> vec;
  bool bRet = scan_directory_tree(dir_name, vec, seqno);
  if (!bRet)
  {
    fprintf(stderr, "[ERROR]: scan director error(%s)\n", strerror(errno));
    return EXIT_FAILURE;
  }
  fprintf(stderr, "[INFO] unlink file count(%lu)\n", vec.size());
  std::string fileName;
  for (size_t i = 0; i < vec.size(); i++)
  {
    fileName = vec.at(i);
    ::unlink(fileName.c_str());
  }
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
  signal(SIGINT, signlHandler);
  std::string dir_name;
  int32_t i = 0;
  int32_t interval = 24 * 60 * 60; //86400(one day)
  while ((i = getopt(argc, argv, "f:i:c:t:h")) != EOF)
  {
    switch (i)
    {
      case 'f':
        dir_name = optarg;
        break;
      case 'i':
        interval = atoi(optarg);
        break;
      case 'h':
      default:
        fprintf(stderr, "Usage: %s -f fileQueueDirPath -i interval\n", argv[0]);
        fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
        fprintf(stderr, "       -i interval: interval time\n");
        fprintf(stderr, "       -h help\n");
        return EXIT_SUCCESS;
    }
  }
  if (dir_name.empty())
  {
    fprintf(stderr, "Usage: %s -f fileQueueDirPath -i interval\n", argv[0]);
    fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
    fprintf(stderr, "       -i interval: interval time\n");
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

  do
  {
    int iret = do_unlink_file(dir_name);
    if (iret != EXIT_SUCCESS)
    {
      fprintf(stderr, "%s:%d [ERROR]: do work error\n", __FILE__, __LINE__);
      break;
    }
    common::Func::sleep(interval, gstop);
  }
  while(!gstop);
  return EXIT_SUCCESS;
}

