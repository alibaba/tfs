/***********************************************************************
 * tfsclient c客户端提供c++和c两种接口, 接口都一一对应.
 * tfsclient为单例，对tfs文件的操作接口, 除去tfs文件名的特殊逻辑外，
 * 基本类似posix的文件处理接口。
 *
 *
 *
 *             c++ 接口处理基本流程为:
 *   // 获得tfsclient单例
 *   TfsClient* tfs_client = TfsClient::Instance();
 *
 *   // 初始化,后两个参数为block cache的过期时间（s)和数目，可以使用默认值
 *   // tfs_client->initialize("127.0.0.1:3100", 1800, 10000)
 *   tfs_client->initialize("127.0.0.1:3100");
 *
 *   // 打开一个tfs文件进行写。写时tfs文件名不指定，后缀名自由选择。
 *   int fd = tfs_client->open(NULL, ".jpg", T_WRITE);
 *
 *   // 写数据
 *   char buf[1024];
 *   tfs_client->write(fd, buf, 1024);
 *
 *   // 关闭文件句柄， 获得tfs文件名
 *   char name[TFS_FILE_LEN];
 *   tfs_client->close(fd, name, TFS_FILE_LEN);
 *
 *   // 打开一个tfs文件进行读。读时tfs文件名必须指定，
 *   // 后缀名或者传入写时指定的后缀，或者不传。
 *   int fd = tfs_client->open(NULL, NULL, T_READ);
 *
 *   // 写数据
 *   char buf[1024];
 *   tfs_client->read(fd, buf, 1024);
 *
 *   // 关闭文件句柄
 *   tfs_client->close(fd);
 *
 *   // 销毁资源
 *   tfs_client->destroy();
 *
 *************************************************************************/


#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include "new_client/tfs_client_api.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class TfsClientTestCXX : public testing::Test
{
public:
  TfsClientTestCXX()
  {
  }
  ~TfsClientTestCXX()
  {
  }

  static void SetUpTestCase();
  static void TearDownTestCase();
  static const char* ns_ip_;
  static char* log_level_;

#ifdef WITH_UNIQUE_STORE
  int op_unique(const int32_t extra_flag);
#endif
  int op_read(const int32_t extra_flag);
  int op_readv2(const int32_t extra_flag);
  int op_pread(const int32_t extra_flag);
  int op_stat(const int32_t extra_flag);
  int op_write(const int32_t extra_flag);
  int op_update(const int32_t extra_flag);
  int op_unlink(const int32_t extra_flag);

  int op_fetch(const int32_t extra_flag);
  void print_info(const char* name, const int status);

protected:
  static TfsClient* tfs_client_;
  static char tfs_name_[TFS_FILE_LEN];
};

const int64_t OP_SIZE = 102400;
const char* key_file = NULL;

const char* TfsClientTestCXX::ns_ip_ = NULL;
char* TfsClientTestCXX::log_level_ = "info";
TfsClient* TfsClientTestCXX::tfs_client_ = NULL;
char  TfsClientTestCXX::tfs_name_[TFS_FILE_LEN];

void TfsClientTestCXX::SetUpTestCase()
{
  // initialize
  tfs_client_ = TfsClient::Instance();
  int ret = TFS_ERROR;

  printf("client %p\n", tfs_client_);
  if ((ret = tfs_client_->initialize(ns_ip_)) != TFS_SUCCESS)
  {
    printf("init tfs client fail, ret: %d\n", ret);
    return;
  }

  // set log level
  tfs_client_->set_log_level(log_level_);
}

void TfsClientTestCXX::TearDownTestCase()
{
  if (tfs_client_ != NULL)
  {
    tfs_client_->destroy();
  }
}

void TfsClientTestCXX::print_info(const char* name, const int status)
{
  printf("=== op %s %s, ret: %d ===\n", name, TFS_SUCCESS == status ? "success" : "fail", status);
}

int TfsClientTestCXX::op_pread(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  // open
  char buf[OP_SIZE*2];
  int fd = tfs_client_->open(tfs_name_, ".jpg", T_READ|extra_flag);
  EXPECT_TRUE(fd > 0);
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE/2, OP_SIZE/3), OP_SIZE/2);
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE/2, OP_SIZE/3), OP_SIZE/2);
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE/2, 0), OP_SIZE/2);
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE, OP_SIZE/2), OP_SIZE/2);
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE/2, OP_SIZE/2), OP_SIZE/2);
  EXPECT_TRUE(tfs_client_->pread(fd, buf, OP_SIZE/2, -1) < 0);
  EXPECT_TRUE(tfs_client_->pread(fd, buf, OP_SIZE/2, OP_SIZE*2) <= 0);

  return TFS_SUCCESS;
}

int TfsClientTestCXX::op_read(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  // open
  int fd = tfs_client_->open(tfs_name_, ".jpg", T_READ|extra_flag);

  EXPECT_TRUE(fd > 0);

  // read
  int64_t len = 0;
  char buf[OP_SIZE];

  len = tfs_client_->read(fd, buf, OP_SIZE/2);

  // lseek then read
  EXPECT_EQ(tfs_client_->lseek(fd, OP_SIZE/3, T_SEEK_CUR), (OP_SIZE/2 + OP_SIZE/3));
  // read
  EXPECT_EQ(tfs_client_->read(fd, buf, OP_SIZE/2), OP_SIZE - OP_SIZE/2 - OP_SIZE/3);
  // pread
  EXPECT_EQ(tfs_client_->pread(fd, buf, OP_SIZE/2, OP_SIZE/3), OP_SIZE/2);
  // close
  EXPECT_EQ(tfs_client_->close(fd), TFS_SUCCESS);

  return TFS_SUCCESS;
}

int TfsClientTestCXX::op_readv2(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  // open
  int fd = tfs_client_->open(tfs_name_, ".jpg", T_READ|extra_flag);
  EXPECT_TRUE(fd > 0);
  // read
  char buf[OP_SIZE];

  TfsFileStat stat_info;
  EXPECT_EQ(tfs_client_->readv2(fd, buf, OP_SIZE/2, &stat_info), OP_SIZE/2);
  EXPECT_EQ(stat_info.size_, OP_SIZE);
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
         tfs_name_, stat_info.file_id_, stat_info.offset_, stat_info.size_, stat_info.usize_,
         stat_info.modify_time_, stat_info.create_time_, stat_info.flag_, stat_info.crc_);

  return TFS_SUCCESS;
}

int TfsClientTestCXX::op_write(const int extra_flag)
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

    fd = tfs_client_->open(NULL, ".jpg", T_WRITE|extra_flag, key_file);
  }
  else
  {
    fd = tfs_client_->open(NULL, ".jpg", T_WRITE|extra_flag);
  }

  EXPECT_TRUE(fd > 0);
  if (fd <= 0)
  {
    printf("get fd for write fail, ret: %d \n", fd);
    return ret;
  }
  // TODO. lseek then write and pwrite not supported now

  // write
  int64_t len = 0;
  char buf[OP_SIZE];

  if ((len = tfs_client_->write(fd, buf, OP_SIZE)) != OP_SIZE)
  {
    printf("write data to tfs fail, ret: %"PRI64_PREFIX"d\n", len);
    // write fail, just close fd
    tfs_client_->close(fd);
    return ret;
  }
  EXPECT_EQ(len, OP_SIZE);

  // write success, close fd and get tfs file name
  if ((ret = tfs_client_->close(fd, tfs_name_, TFS_FILE_LEN)) != TFS_SUCCESS)
  {
    printf("close tfs file fail, ret: %d\n", ret);
    return ret;
  }
  EXPECT_EQ(ret, TFS_SUCCESS);
  return ret;
}

int TfsClientTestCXX::op_update(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  // open
  int ret = TFS_ERROR;
  int fd = tfs_client_->open(tfs_name_, ".jpg", T_WRITE|extra_flag);
  if (extra_flag != T_LARGE)
  {
    EXPECT_TRUE(fd > 0);
  }
  else
  {
    EXPECT_FALSE(fd > 0);
    return  TFS_SUCCESS;
  }

  // write
  int64_t len = 0;
  char buf[OP_SIZE];
  EXPECT_EQ((len = tfs_client_->write(fd, buf, OP_SIZE)), OP_SIZE);
  if (len != OP_SIZE)
  {
    printf("write data to tfs fail, ret: %"PRI64_PREFIX"d\n", len);
    // write fail, just close fd
    EXPECT_TRUE(tfs_client_->close(fd) != TFS_SUCCESS);
    return ret;
  }
  EXPECT_EQ(tfs_client_->close(fd, tfs_name_, TFS_FILE_LEN), TFS_SUCCESS);
  return ret;
}

int TfsClientTestCXX::op_stat(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  int fd = tfs_client_->open(tfs_name_, NULL, T_STAT|extra_flag);

  EXPECT_TRUE(fd > 0);
  TfsFileStat stat_info;
  // default NORMAL_STAT
  // tfs_client_->fstat(fd, &stat_info, NORMAL_STAT)
  EXPECT_EQ(tfs_client_->fstat(fd, &stat_info), TFS_SUCCESS);
  EXPECT_EQ(stat_info.size_, OP_SIZE);
  EXPECT_TRUE(stat_info.usize_ > OP_SIZE);
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
         tfs_name_, stat_info.file_id_, stat_info.offset_, stat_info.size_, stat_info.usize_,
         stat_info.modify_time_, stat_info.create_time_, stat_info.flag_, stat_info.crc_);

  return TFS_SUCCESS;
}

int TfsClientTestCXX::op_unlink(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  print_info(tfs_name_, TFS_SUCCESS);
  // 当前大文件不支持undelete

  int64_t file_size = 0;
  file_size = 0;
  // conceal
  EXPECT_EQ(tfs_client_->unlink(file_size, tfs_name_, NULL, CONCEAL), TFS_SUCCESS);
  EXPECT_EQ(file_size, 0);
  EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, CONCEAL), TFS_SUCCESS);

  file_size = 0;
  // reveal
  EXPECT_EQ(tfs_client_->unlink(file_size, tfs_name_, NULL, REVEAL), TFS_SUCCESS);
  EXPECT_EQ(file_size, 0);
  EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, REVEAL), TFS_SUCCESS);

  file_size = 0;
  // conceal
  EXPECT_EQ(tfs_client_->unlink(file_size, tfs_name_, NULL, CONCEAL), TFS_SUCCESS);
  EXPECT_EQ(file_size, 0);
  EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, CONCEAL), TFS_SUCCESS);

  file_size = 0;
  // delete
  EXPECT_EQ(tfs_client_->unlink(file_size, tfs_name_, NULL, DELETE), TFS_SUCCESS);
  EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, DELETE), TFS_SUCCESS);
  EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL), TFS_SUCCESS);

  if (extra_flag == T_LARGE)
  {
    EXPECT_TRUE(file_size > OP_SIZE);
    // undelete
    EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, UNDELETE), TFS_SUCCESS);
  }
  else
  {
    EXPECT_EQ(file_size, OP_SIZE);
    file_size = 0;
    // undelete
    EXPECT_EQ(tfs_client_->unlink(file_size, tfs_name_, NULL, UNDELETE), TFS_SUCCESS);
    EXPECT_EQ(file_size, OP_SIZE);
    EXPECT_NE(tfs_client_->unlink(file_size, tfs_name_, NULL, UNDELETE), TFS_SUCCESS);
  }

  return TFS_SUCCESS;
}

int TfsClientTestCXX::op_fetch(const int extra_flag)
{
  EXPECT_EQ(op_write(extra_flag), TFS_SUCCESS);
  char buf[OP_SIZE*2];
  int64_t ret_count = 0;
  EXPECT_EQ(tfs_client_->fetch_file(ret_count, buf, OP_SIZE*2, tfs_name_, NULL, NULL), TFS_SUCCESS);
  EXPECT_EQ(ret_count, OP_SIZE);
  EXPECT_EQ(tfs_client_->fetch_file(ret_count, buf, OP_SIZE, tfs_name_, NULL, NULL), TFS_SUCCESS);
  EXPECT_EQ(ret_count, OP_SIZE);
  EXPECT_EQ(tfs_client_->fetch_file(ret_count, buf, OP_SIZE/2, tfs_name_, NULL, NULL), TFS_SUCCESS);
  EXPECT_EQ(ret_count, OP_SIZE/2);

  return TFS_SUCCESS;;
}

#ifdef WITH_UNIQUE_STORE
int TfsClientTestCXX::op_unique(const int extra_flag)
{
  if (extra_flag == T_LARGE)
  {
    return TFS_SUCCESS;
  }

  int64_t file_size = 0;
  char unique_name[TFS_FILE_LEN];
  char buf[OP_SIZE] = {'a', 'b', 'c', '8', '=', '&'};

  EXPECT_TRUE(tfs_client_->save_unique(unique_name, TFS_FILE_LEN, buf, OP_SIZE, ".am") <= 0);
  EXPECT_EQ(tfs_client_->init_unique_store("tair2.config-vip.taobao.net:5198", "tair2.config-vip.taobao.net:5198",
                                           "group_1", 100), TFS_SUCCESS);
  // clear
  EXPECT_EQ(tfs_client_->save_unique(unique_name, TFS_FILE_LEN, buf, OP_SIZE, ".am"), OP_SIZE);
  file_size = 0;
  EXPECT_EQ(tfs_client_->unlink_unique(file_size, unique_name, NULL, NULL, 102450000), 0);
  EXPECT_EQ(file_size, OP_SIZE);
  // clear end

  EXPECT_EQ(tfs_client_->save_unique(unique_name, TFS_FILE_LEN, buf, OP_SIZE, ".am"), OP_SIZE);
  char reunique_name[TFS_FILE_LEN];
  int32_t times = 10;

  for (int32_t i = 0; i < times; i++)
  {
    EXPECT_EQ(tfs_client_->save_unique(reunique_name, TFS_FILE_LEN, buf, OP_SIZE, ".am"), OP_SIZE);
    EXPECT_STREQ(unique_name, reunique_name);
  }

  // refcount = times + 1
  for (int32_t i = times; i >= 0; i--)
  {
    file_size = 0;
    EXPECT_EQ(tfs_client_->unlink_unique(file_size, unique_name), i);
    EXPECT_EQ(file_size, OP_SIZE);
  }
  file_size = 0;
  EXPECT_TRUE(tfs_client_->unlink_unique(file_size, unique_name) < 0);
  EXPECT_EQ(file_size, 0);

  return TFS_SUCCESS;
}

TEST_F(TfsClientTestCXX, test_unique)
{
  op_unique(T_DEFAULT);
  op_unique(T_LARGE);
}
#endif

TEST_F(TfsClientTestCXX, test_write)
{
  op_write(T_DEFAULT);
  op_write(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_update)
{
  op_update(T_DEFAULT);
  op_update(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_fetch)
{
  op_fetch(T_DEFAULT);
  op_fetch(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_read)
{
  op_read(T_DEFAULT);
  op_read(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_readv2)
{
  op_readv2(T_DEFAULT);
  op_readv2(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_pread)
{
  op_pread(T_DEFAULT);
  op_pread(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_stat)
{
  op_stat(T_DEFAULT);
  op_stat(T_LARGE);
}

TEST_F(TfsClientTestCXX, test_unlink)
{
  op_unlink(T_DEFAULT);
  op_unlink(T_LARGE);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);

  int32_t i = 0;
  while ((i = getopt(argc, argv, "s:l:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      TfsClientTestCXX::ns_ip_ = optarg;
      break;
    case 'l':
      TfsClientTestCXX::log_level_ = optarg;
      break;
    case 'h':
    default:
      printf("Usage: %s -s nsip -l loglevel\n", argv[0]);
      return EXIT_FAILURE;
    }
  }

  if (NULL == TfsClientTestCXX::ns_ip_)
  {
    printf("Usage: %s -s nsip -l loglevel\n", argv[0]);
    return EXIT_FAILURE;
  }

  key_file = argv[0];

  return RUN_ALL_TESTS();
}
