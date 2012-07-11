/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: util.cpp 155 2011-02-21 07:33:27Z zongdai@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "util.h"
#include "new_client/fsname.h"
#include "common/func.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

int fetch_input_opt(int argc, char** argv, ThreadParam& param, int& thread_count)
{
  int i = 0;
  string range, mins, maxs;
  //size = 0; count = 0; thread = 0;
  while ((i = getopt(argc, argv, "d:f:c:n:t:p:r:s:o:i:")) != EOF)
  {
    switch (i)
    {
    case 'd':
      {
        param.ns_ip_port_ = optarg;
        printf("%s\n", param.ns_ip_port_.c_str());
        string::size_type tmppos = param.ns_ip_port_.find_first_of(":");
        if (string::npos == tmppos)
        {
          return EXIT_FAILURE;
        }
      }
      break;
    case 'f':
      param.conf_ = optarg;
      printf("%s\n", param.conf_.c_str());
      break;
    case 'c':
      param.file_count_ = atoi(optarg);
      printf("%d\n", param.file_count_);
      break;
    case 'n':
      param.file_size_ = atoi(optarg);
      break;
    case 't':
      thread_count = atoi(optarg);
      break;
    case 'r':
      {
        range = string(optarg);
        string::size_type pos = range.find_first_of(":");
        if (string::npos == pos)
        {
          return EXIT_FAILURE;
        }
        mins = range.substr(0, pos);
        maxs = range.substr(pos + 1, string::npos);
        param.min_size_ = atoi(mins.c_str());
        param.max_size_ = atoi(maxs.c_str());
        printf("%d:%d\n", param.min_size_, param.max_size_);
        //printf("range : %s,mins : %s, maxs : %s,min_size : %d,max_size : %d\n",range.c_str(),mins.c_str(),maxs.c_str(),param.min_size,param.max_size);
      }
      break;
    case 'p':
      param.profile_ = atoi(optarg);
      break;
    case 's':
      param.random_ = atoi(optarg);
      break;
    case 'i':
      param.sample_file_ = optarg;
      break;
    case 'o':
      param.oper_ratio_ = optarg;
      break;
    default:
      return EXIT_FAILURE;
    }
  }
  return EXIT_SUCCESS;
}

uint32 generate_data(char* buf, uint32 size)
{
  srand(time(NULL) + rand() + pthread_self());

  for (uint32_t i = 0; i < size; i++)
  {
    buf[i] = static_cast<char> (rand() % 90 + 32);
  }
  return size;
}

int read_local_file(const char* filename, char* data, uint32_t& length)
{
  int fd = open(filename, O_RDONLY, 0600);
  if (fd == -1)
  {
    printf("file %s open failed, %s. \n", filename, strerror(errno));
    return -1;
  }

  struct stat statbuf;
  int ret = fstat(fd, &statbuf);
  if (ret)
  {
    printf("file %s stat failed, %s.\n", filename, strerror(errno));
    return -1;
  }
  if ((uint32_t) statbuf.st_size > length)
  {
    printf("file %s size %u, large than input data length %u.\n", filename, (uint32_t) statbuf.st_size, length);
    return -1;
  }
  length = statbuf.st_size;
  ret = read(fd, data, statbuf.st_size);
  if (ret != statbuf.st_size)
  {
    printf("file %s size :%u, readed:%u\n", filename, (uint32_t) statbuf.st_size, ret);
    return -1;
  }
  return 0;
}

int retry_open_file(TfsClient* tfsclient, char* filename, char* prefix, int mode, int& fd)
{
  int retry = 0;
  int ret;
  while (retry++ < RETRY_TIMES)
  {
    ret = tfsclient->open(filename, prefix, mode);
    if (ret > 0)
    {
      fd = ret;
      ret = EXIT_SUCCESS;
      break;
    }
    //else return ret;
    sleep(1);
  }
  return ret;
}

void print_rate(unsigned int num, double time_taken)
{
  printf("rate: %10.5f (Mbps)\n", (((double) num * 8.0) / time_taken) / (1024.0 * 1024.0));
  printf("rate: %10.5f (MBps)\n", ((double) num / time_taken) / (1024.0 * 1024.0));
}

uint32 convname(const char* tfsname, char* prefix, uint32_t& blockid, uint64_t& fileid)
{
  FSName name(tfsname, prefix, 0);
  blockid = name.get_block_id();
  fileid = name.get_file_id();
  return blockid;
}

int write_data(TfsClient* tfsclient, int fd, char* data, uint32_t length)
{
  int num_wrote = 0;
  int left = length - num_wrote;

  int ret = EXIT_SUCCESS;

  while (left > 0)
  {
    //printf("begin write data(%d)\n", left);
    ret = tfsclient->write(fd, data + num_wrote, left);
    //printf("write data completed(%d)\n", ret);
    if (ret < 0)
    {
      return ret;
    }

    if (ret >= left)
    {
      return left;
    }
    else
    {
      num_wrote += ret;
      left = length - num_wrote;
    }
  }

  return num_wrote;
}

int copy_file(TfsClient* tfsclient, char* tfsname, int local_fd)
{

  //char tmpstr[32];
  char *prefix = NULL;
  if (static_cast<int32_t> (strlen(tfsname)) < FILE_NAME_LEN || (tfsname[0] != 'T' && tfsname[0] != 'L'))
  {
    fprintf(stderr, "TFS �ļ���Ƿ�%s\n", tfsname);
    return EXIT_FAILURE;
  }
  prefix = tfsname + FILE_NAME_LEN;

  int fd;
  if ((fd = tfsclient->open(tfsname, prefix, T_READ)) <= 0 )
  {
    fprintf(stderr, "open tfsfile fail\n");
    return EXIT_FAILURE;
  }

  TfsFileStat fstat;
  if (tfsclient->fstat(fd, &fstat) == TFS_ERROR)
  {
    tfsclient->close(fd);
    fprintf(stderr, "fstat tfsfile fail\n");
    return EXIT_FAILURE;
  }

  char data[MAX_READ_SIZE];
  uint32_t crc = 0;
  int64_t total_size = 0;
  for (;;)
  {
    int rlen = tfsclient->read(fd, data, MAX_READ_SIZE);
    if (rlen < 0)
    {
      fprintf(stderr, "read tfsfile fail\n");
      tfsclient->close(fd);
      return EXIT_FAILURE;
    }

    if (rlen == 0)
    {
      break;
    }

    if (local_fd != -1 && write(local_fd, data, rlen) != rlen)
    {
      fprintf(stderr, "write local file fail.\n");
      tfsclient->close(fd);
      return EXIT_FAILURE;
    }

    crc = Func::crc(crc, data, rlen);
    total_size += rlen;
    if (rlen != (int32_t) MAX_READ_SIZE)
      break;
  }

  tfsclient->close(fd);
  if (crc != fstat.crc_ || total_size != fstat.size_)
  {
    fprintf(stderr, "crc error: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d\n",
        crc, fstat.crc_, total_size, fstat.size_);
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

/*int copy_file_v3(TfsClient &tfsclient, char* tfsname, uint32_t width, uint32_t height, int local_fd, bool& zoomed)
{
  //char tmpstr[32];
  char *prefix = NULL;
  if (static_cast<int32_t> (strlen(tfsname)) < FILE_NAME_LEN || tfsname[0] != 'T' || tfsname[0] != 'L')
  {
    fprintf(stderr, "TFS �ļ���Ƿ�%s\n", tfsname);
    return EXIT_FAILURE;
  }
  prefix = tfsname + FILE_NAME_LEN;

  int fd;
  if (fd = tfsclient.open(tfsname, prefix, T_READ) <= 0)
  {
    fprintf(stderr, "open tfsfile fail\n");
    return EXIT_FAILURE;
  }

  TfsFileStat fstat;
  memset(&fstat, 0, sizeof(TfsFileStat));
  char data[MAX_READ_SIZE];
  uint32_t crc = 0;
  int total_size = 0;
  for (;;)
  {
    int32_t rlen = tfsfile.tfs_read_scale_image(data, MAX_READ_SIZE, width, height, &finfo);
    if (rlen < 0)
    {
      fprintf(stderr, "read tfsfile fail: %s\n", tfsfile.get_error_message());
      tfsfile.tfs_close();
      return EXIT_FAILURE;
    }
    if (rlen == 0)
    {
      break;
    }

    if (local_fd != -1 && write(local_fd, data, rlen) != rlen)
    {
      fprintf(stderr, "write local file fail.\n");
      tfsfile.tfs_close();
      return EXIT_FAILURE;
    }
    crc = Func::crc(crc, data, rlen);
    total_size += rlen;

    if (finfo.usize_ - finfo.size_ > (int32_t) sizeof(FileInfo))
    {
      zoomed = true;
    }
    else
    {
      zoomed = false;
    }

    if (tfsfile.is_eof())
    {
      break;
    }

    if (rlen != (int) MAX_READ_SIZE)
    {
      break;
    }

    if (total_size >= finfo.size_)
    {
      break;
    }
  }

  tfsfile.tfs_close();
  if (crc != finfo.crc_ || total_size != finfo.size_)
  {
    fprintf(stderr, "%s, crc error: %u <> %u, size: %u <> %u\n", tbsys::CNetUtil::addrToString(
        tfsfile.get_last_elect_ds_id()).c_str(), crc, finfo.crc_, total_size, finfo.size_);
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
*/
int copy_file_v2(TfsClient* tfsclient, char* tfsname, int local_fd)
{

  //char tmpstr[32];
  char *prefix = NULL;
  if (static_cast<int32_t> (strlen(tfsname)) < FILE_NAME_LEN || (tfsname[0] != 'T' && tfsname[0] != 'L'))
  {
    fprintf(stderr, "TFS �ļ���Ƿ�%s\n", tfsname);
    return EXIT_FAILURE;
  }
  prefix = tfsname + FILE_NAME_LEN;

  int fd;
  if ((fd = tfsclient->open(tfsname, prefix, T_READ)) <= 0)
  {
    fprintf(stderr, "open tfsfile fail\n");
    return EXIT_FAILURE;
  }

  TfsFileStat fstat;
  memset(&fstat, 0, sizeof(TfsFileStat));
  char data[MAX_READ_SIZE];
  uint32_t crc = 0;
  int64_t total_size = 0;
  for (;;)
  {
    int32_t rlen = tfsclient->readv2(fd, data, MAX_READ_SIZE, &fstat);
    if (rlen < 0)
    {
      fprintf(stderr, "read tfsfile fail\n");
      tfsclient->close(fd);
      return EXIT_FAILURE;
    }
    if (rlen == 0)
      break;
    if (local_fd != -1 && write(local_fd, data, rlen) != rlen)
    {
      fprintf(stderr, "write local file fail.\n");
      tfsclient->close(fd);
      return EXIT_FAILURE;
    }
    crc = Func::crc(crc, data, rlen);
    total_size += rlen;
    if (rlen != (int) MAX_READ_SIZE)
      break;
    if (total_size >= fstat.size_)
      break;
  }

  tfsclient->close(fd);
  if (crc != fstat.crc_ || total_size != fstat.size_)
  {
    fprintf(stderr, "crc error: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d\n", 
        crc, fstat.crc_, total_size, fstat.size_);
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

int read_data(TfsClient* tfsclient, char* filename)
{

  int ret = 0;

  int fd;
  fd = tfsclient->open(filename, NULL, T_READ);
  if (fd <= 0)
  {
    printf("tfsopen failed:(%s)\n", filename);
    return fd;
  }

  TfsFileStat fstat;
  ret = tfsclient->fstat(fd, &fstat);

  if (ret == EXIT_FAILURE)
  {
    printf("tfsstat failed:(%d)\n", ret);
    tfsclient->close(fd);
    return ret;
  }

  int file_size = fstat.size_;
  char* buffer = new char[file_size + 1];
  memset(buffer, 0, file_size + 1);

  int num_readed = 0;
  int num_per_read = min(file_size, BUFFER_SIZE);
  uint32 crc = 0;

  do
  {
    ret = tfsclient->read(fd, buffer + num_readed, num_per_read);
    if (ret < 0)
      break;
    //if (ret != num_per_read) break;
    crc = Func::crc(crc, buffer + num_readed, ret);
    //printf("read file content:(\n%s)\n, already readed (%d)\n", buffer+num_readed, num_readed+ret);
    num_readed += ret;
    if (num_readed >= file_size)
      break;
  }
  while (1);

  if (ret < -1 || num_readed < file_size)
  {
    printf("tfsread failed (%d), readed(%d)\n", ret, num_readed);
    ret = EXIT_FAILURE;
    goto error;
  }

  if (crc != fstat.crc_)
  {
    printf("crc check failed (%d), info.crc(%d)\n", crc, fstat.crc_);
    ret = EXIT_FAILURE;
    goto error;
  }

  error: delete[] buffer;
  tfsclient->close(fd);
  if (ret < 0)
    return ret;
  return num_readed;
}

///-----------------------------------------------------------

Timer::Timer()
{
}

int64_t Timer::current()
{
  struct timeval s_start;
  gettimeofday(&s_start, NULL);
  return s_start.tv_sec * 1000000LL + s_start.tv_usec;
}

int64_t Timer::start()
{
  start_ = current();
  return start_;
}

int64_t Timer::consume()
{
  end_ = current();
  return end_ - start_;
}

double calc_iops(int32_t count, int64_t consumed)
{
  if (consumed == 0)
  {
    return 0.0;
  }
  return (double) count / ((double) consumed / 1000000);
}

double calc_rate(int32_t count, int64_t consumed)
{
  if (count == 0)
  {
    return 0.0;
  }
  return (double) consumed / count;
}

void TimeConsumed::process()
{
  if (time_consumed_ < min_time_consumed_)
  {
    min_time_consumed_ = time_consumed_;
  }

  if (time_consumed_ > max_time_consumed_)
  {
    max_time_consumed_ = time_consumed_;
  }

  accumlate_time_consumed_ += time_consumed_;
}

void TimeConsumed::display()
{
  /*
   printf("------ | ---------  | --------- | --------- | --------- | ---------\n");
   printf("TYPE   | SUCCCOUNT  | FAILCOUNT | AVG       | MIN       | MAX      \n");
   printf("------ | ---------  | --------- | --------- | --------- | ---------\n");
   */
  printf("%-6s | %-9d | %-9d | %-9" PRI64_PREFIX "d | %-9" PRI64_PREFIX "d | %-9" PRI64_PREFIX "d\n", consume_name_.c_str(), succ_count(), fail_count_, avg(),
      min_time_consumed_, max_time_consumed_);
  //	printf("------ | ---------  | --------- | --------- | --------- | ---------\n");
  return;
}

int Stater::stat_time_count(uint64_t cost_time)
{
  uint32_t key = time_category(cost_time);
  map<uint32_t, uint64_t>::iterator it = time_stat_map_.find(key);
  if (it != time_stat_map_.end())
  {
    it->second++;
    return static_cast<int> (it->second);
  }
  else
  {
    time_stat_map_[key] = 1;
    return 1;
  }
  return 0;
}

uint32_t Stater::time_category(uint64_t cost_time)
{
  uint32_t key = static_cast<uint32_t> (cost_time / 1000);
  if (key >= 100 && key < 1000)
  {
    //100ms--1s
    key = key / 100 * 100;
  }
  else if (key >= 1000)
  {
    // 1s
    key = 1000;
  }
  else //0-100ms
  {
    key = key / 10 * 10;
  }

  return key;
}

void Stater::dump_time_stat()
{
  map<uint32_t, uint64_t>::iterator mit = time_stat_map_.begin();
  //	printf("-----BEGIN DUMP_TIME_STAT-----\n");
  printf("--------- | -----------  | ----\n");
  printf("OPER_NAME | OPER_TIME    | NUMS\n");
  printf("--------- | -----------  | ----\n");
  for (; mit != time_stat_map_.end(); mit++)
  {
    printf("%-9s | %-12s | %-10" PRI64_PREFIX "u\n", stat_name_.c_str(), time_category_desc(mit->first).c_str(), mit->second);
  }
  printf("--------- | -----------  | ----\n\n");
  //	printf("-----END DUMP_TIME_STAT-------\n\n");
}

string Stater::time_category_desc(uint32_t category)
{
  char buf[32];
  if (category < 100)
  {
    uint32_t base = category / 10;
    sprintf(buf, "%ums~%ums", category, (base + 1) * 10);
  }
  else if (category >= 100 & category < 1000)
  {
    uint32_t base = category / 100;
    sprintf(buf, "%ums~%ums", category, (base + 1) * 100);
  }
  else if (category >= 1000)
  {
    return ">1s";
  }
  return string(buf);
}
