/***********************************************************************
 * tfsclient c客户端提供c++和c两种接口, 接口都一一对应.
 * tfsclient为单例，对tfs文件的操作接口, 除去tfs文件名的特殊逻辑外，
 * 基本类似posix的文件处理接口。
 *
 *
 *
 *             c 接口处理基本流程为:
 *
 *   // 初始化,后两个参数为block cache的过期时间（s)和数目
 *   t_initialize("127.0.0.1:3100", 1800, 500000);
 *
 *   // 打开一个tfs文件进行写。写时tfs文件名不指定，后缀名自由选择。
 *   // 当以T_LARGE|T_WRITE flag打开时，最后一个参数key(后面详述）。
 *   // 当多集群处理时，提供t_open2
 *   // t_open2(NULL, ".jpg", "127.0.1.1:3100", T_WRITE, NULL)
 *   int fd = t_open(NULL, ".jpg", T_WRITE, NULL);
 *
 *   // 写数据
 *   char buf[1024];
 *   t_write(fd, buf, 1024);
 *
 *   // 关闭文件句柄， 获得tfs文件名
 *   char name[TFS_FILE_LEN];
 *   t_close(fd, name, TFS_FILE_LEN);
 *
 *   // 打开一个tfs文件进行读。读时tfs文件名必须指定，
 *   // 后缀名或者传入写时指定的后缀，或者不传。
 *   fd = t_open(name, NULL, NULL, T_READ, NULL);
 *
 *   // 读数据
 *   t_read(fd, buf, 1024);
 *
 *   // 关闭文件句柄
 *   t_close(fd, NULL, 0);
 *
 *   // 销毁资源
 *   t_destroy();
 *
 *   其他接口类似。下面demo展示。
 *************************************************************************/

#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "tfs_client_capi.h"

typedef enum
{
  TFS_LARGE_FILE = 0,
  TFS_SMALL_FILE
} FileType;

char tfs_name[TFS_FILE_LEN];
const int64_t OP_SIZE = 1024;
const char* key_file = NULL;

void print_info(const char* name, const int status)
{
  printf("=== op %s %s, ret: %d ===\n", name, TFS_SUCCESS == status ? "success" : "fail", status);
}

int op_read(const int extra_flag)
{
  // open
  int fd = t_open(tfs_name, ".jpg", T_READ|extra_flag, NULL);

  if (fd <= 0)
  {
    printf("get fd for read fail, ret: %d \n", fd);
    return TFS_ERROR;
  }

  // read
  int64_t len = 0;
  char buf[OP_SIZE];

  if ((len = t_read(fd, buf, OP_SIZE/2)) < 0)
  {
    printf("read data fail, ret: %"PRI64_PREFIX"d\n", len);
    return TFS_ERROR;
  }

  // lseek then read
  t_lseek(fd, OP_SIZE/3, T_SEEK_CUR);
  // read
  if ((len = t_read(fd, buf, OP_SIZE/2)) < 0)
  {
    printf("lseek and read data fail, ret: %"PRI64_PREFIX"d\n", len);
    return TFS_ERROR;
  }

  // pread
  if ((len = t_pread(fd, buf, OP_SIZE/2, OP_SIZE/3)) < 0)
  {
    printf("pread data fail, ret: %"PRI64_PREFIX"d\n", len);
    return TFS_ERROR;
  }

  // close
  t_close(fd, NULL, 0);

  return TFS_SUCCESS;
}

int op_write(const int extra_flag)
{
  // open
  int ret = TFS_ERROR;
  int fd = 0;
  if (extra_flag == T_LARGE)
  {
    // 打开一个tfs文件写，为了断点续传，必须传入一个key参数，来标识此次大文件的写。
    // 一次写失败后，再次传入相同的key写，tfsclient会根据key找到前一次已经写完成的
    // 部分进行重用。
    // 为了确保key的唯一性，当前tfsclient接受的key必须是本地文件系统上存在的一个文件路径,
    // 该key文件的内容无所谓.

    fd = t_open(NULL, ".jpg", T_WRITE|extra_flag, key_file);
  }
  else
  {
    fd = t_open(NULL, ".jpg", T_WRITE|extra_flag, NULL);
  }

  if (fd <= 0)
  {
    printf("get fd for write fail, ret: %d \n", fd);
    return ret;
  }
  // lseek then write and pwrite not supported now

  // write
  int64_t len = 0;
  char buf[OP_SIZE];

  if ((len = t_write(fd, buf, OP_SIZE)) != OP_SIZE)
  {
    printf("write data to tfs fail, ret: %"PRI64_PREFIX"d\n", len);
    // write fail, just close fd
    t_close(fd, NULL, 0);
    return ret;
  }

  // write success, close fd and get tfs file name
  if ((ret = t_close(fd, tfs_name, TFS_FILE_LEN)) != TFS_SUCCESS)
  {
    printf("close tfs file fail, ret: %d\n", ret);
    return ret;
  }

  return ret;
}

int op_stat(const int extra_flag)
{
  int fd = t_open(tfs_name, NULL, T_STAT|extra_flag, NULL);

  if (fd <= 0)
  {
    printf("get fd for stat fail, ret: %d\n", fd);
    return TFS_ERROR;
  }

  TfsFileStat stat_info;
  int ret = t_fstat(fd, &stat_info, NORMAL_STAT);
  if (ret != TFS_SUCCESS)
  {
    printf("stat tfs file, ret: %d\n", ret);
  }
  else
  {
    printf("stat tfs file success:\n"
           "filename: %s\n"
           "fileid: %"PRI64_PREFIX"u\n"
           "offset: %d\n"
           "size: %"PRI64_PREFIX"d\n"
           "usize: %"PRI64_PREFIX"d\n"
           "modify_time: %d\n"
           "create_time: %d\n"
           "status: %d\n"
           "crc: %u\n",
           tfs_name, stat_info.file_id_, stat_info.offset_, stat_info.size_, stat_info.usize_,
           stat_info.modify_time_, stat_info.create_time_, stat_info.flag_, stat_info.crc_);
  }

  t_close(fd, NULL, 0);

  return TFS_SUCCESS;
}


int op_unlink(const int extra_flag)
{
  // 当前大文件不支持undelete

  int ret = TFS_ERROR;

  int64_t file_size = 0;
  file_size = 0;
  // conceal
  ret = t_unlink(tfs_name, NULL, &file_size, CONCEAL);
  if (ret != TFS_SUCCESS)
  {
    printf("conceal file fail, ret: %d\n", ret);
    return ret;
  }
  else
  {
    printf("conceal file size : %"PRI64_PREFIX"d\n", file_size);
  }

  file_size = 0;
  // reveal
  ret = t_unlink(tfs_name, NULL, &file_size, REVEAL);
  if (ret != TFS_SUCCESS)
  {
    printf("reveal file fail, ret: %d\n", ret);
    return ret;
  }
  else
  {
    printf("reveal file size : %"PRI64_PREFIX"d\n", file_size);
  }

  file_size = 0;
  // conceal
  ret = t_unlink(tfs_name, NULL, &file_size, CONCEAL);
  if (ret != TFS_SUCCESS)
  {
    printf("conceal file fail, ret: %d\n", ret);
    return ret;
  }
  else
  {
    printf("conceal file size : %"PRI64_PREFIX"d\n", file_size);
  }

  file_size = 0;
  // delete
  ret = t_unlink(tfs_name, NULL, &file_size, DELETE);
  if (ret != TFS_SUCCESS)
  {
    printf("delete file fail, ret: %d\n", ret);
    return ret;
  }
  else
  {
    printf("delete file size : %"PRI64_PREFIX"d\n", file_size);
  }

  // undelete
  if (extra_flag != T_LARGE)
  {
    file_size = 0;
    ret = t_unlink(tfs_name, NULL, &file_size, UNDELETE);
    if(ret != TFS_SUCCESS)
    {
      printf("undelete file fail,, ret: %d\n", ret);
      return ret;
    }
    else
    {
      printf("undelete file size : %"PRI64_PREFIX"d\n", file_size);
    }
  }
  return TFS_SUCCESS;
}

int op_unique(const int extra_flag)
{
  int64_t ret = TFS_SUCCESS;
  int64_t file_size = 0;
  if ((extra_flag & T_LARGE) == 0)
  {
    int32_t refcnt = 0;
    char unique_name[TFS_FILE_LEN];
    ret = t_init_unique_store("tair2.config-vip.taobao.net:5198", "tair2.config-vip.taobao.net:5198",
                              "group_1", 100, NULL);
    if (ret != TFS_SUCCESS)
    {
      printf("init unique store fail\n");
    }
    else
    {
      // clear
      ret = t_save_unique_file(unique_name, TFS_FILE_LEN, "./Makefile", ".am", NULL);
      if (ret < 0)
      {
        printf("save unique fail. ret: %"PRI64_PREFIX"d\n", ret);
      }
      else
      {
        file_size = 0;
        refcnt = t_unlink_unique(&file_size, unique_name, NULL, NULL, 102450000);
        if (refcnt != 0)
        {
          printf("unlink fail\n");
          return TFS_ERROR;
        }
        else
        {
          printf("unlink unique file size : %"PRI64_PREFIX"d\n", file_size);
        }
      }
      // clear end

      ret = t_save_unique_file(unique_name, TFS_FILE_LEN, "./Makefile", ".am", NULL);
      if (ret < 0)
      {
        printf("save unique fail. ret: %"PRI64_PREFIX"d\n", ret);
        return ret;
      }
      printf("save unique success.name: %s\n", unique_name);

      char reunique_name[TFS_FILE_LEN];
      int32_t times = 10;

      int32_t i = 0;
      for (i = 0; i < times; i++)
      {
        ret = t_save_unique_file(reunique_name, TFS_FILE_LEN, "./Makefile", ".am", NULL);
        if (ret < 0)
        {
          printf("save unique fail. ret: %"PRI64_PREFIX"d\n", ret);
          break;
        }
        else
        {
          printf("resave unique success. %d name: %s\n", i, reunique_name);
          if (memcmp(unique_name, reunique_name, TFS_FILE_LEN) != 0)
          {
            printf("mismatch %s <> %s\n", unique_name, reunique_name);
          }
        }
      }

      // refcount = times + 1
      i = 0;
      for (i = times; i >= 0; i--)
      {
        file_size = 0;
        refcnt = t_unlink_unique(&file_size, unique_name, NULL, NULL, 1);
        if (refcnt != i)
        {
          printf("unlink fail, refcnt %d <> %d\n", refcnt, i);
          return TFS_ERROR;
        }
        else
        {
          printf("unlink unique size : %"PRI64_PREFIX"d\n", file_size);
        }
      }
      file_size = 0;
      refcnt = t_unlink_unique(&file_size, unique_name, NULL, NULL, 1);
      if (refcnt >= 0 || file_size != 0)
      {
        printf("unlink fail, file_size %"PRI64_PREFIX"d\n", file_size);
        return TFS_ERROR;
      }
    }
  }
  return TFS_SUCCESS;
}

int op_tfs_file(const FileType type)
{
  int extra_flag = 0;

  if (type == TFS_LARGE_FILE)
  {
    extra_flag = T_LARGE;
  }
  else if (type != TFS_SMALL_FILE)
  {
    printf("unknown type: %d\n", type);
    return TFS_ERROR;
  }

  printf("========== op demo type: tfs %s file ===========\n", type == TFS_LARGE_FILE ? "large" : "small");

  // write
  int ret = op_write(extra_flag);
  print_info("write", ret);

  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  printf("write tfs file name: %s\n", tfs_name);

  // read
  ret = op_read(extra_flag);
  print_info("read", ret);

  // stat
  ret = op_stat(extra_flag);
  print_info("stat", ret);

  // unlink
  ret = op_unlink(extra_flag);
  print_info("unlink", ret);

  ret = op_unique(extra_flag);
  print_info("unique", ret);

  return ret;
}

int main(int argc, char* argv[])
{
  int32_t i = 0;
  const char* ns_ip = NULL;
  char* log_level = "info";

  while ((i = getopt(argc, argv, "s:l:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      ns_ip = optarg;
      break;
    case 'l':
      log_level = optarg;
      break;
    case 'h':
    default:
      printf("Usage: %s -s nsip\n", argv[0]);
      return EXIT_FAILURE;
    }
  }

  if (NULL == ns_ip)
  {
    printf("Usage: %s -s nsip\n", argv[0]);
    return EXIT_FAILURE;
  }

  int ret = TFS_ERROR;

  /* initialize */
  if ((ret = t_initialize(ns_ip, 1800, 50000, 1)) != TFS_SUCCESS)
  {
    printf("init tfs client fail, ret: %d\n", ret);
    return ret;
  }

  // set log level
  t_set_log_level(log_level);

  key_file = argv[0];

  // demo small file
  op_tfs_file(TFS_SMALL_FILE);

  // demo large file
  op_tfs_file(TFS_LARGE_FILE);

  printf("cache hit ratio: %d\n", t_get_cache_hit_ratio());
  printf("cache hit ratio: %d\n", t_get_cache_hit_ratio());

  /* destroy */
  t_destroy();
  return TFS_SUCCESS;
}
