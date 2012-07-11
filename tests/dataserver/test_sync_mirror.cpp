/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_meta.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <vector>
#include "dataserver_define.h"
#include "common/define.h"
#include "dataservice.h"
#include "blockfile_manager.h"

using namespace tfs::dataserver;
using namespace tfs::common;

DataService g_data_service;

const char* DIR_MIRROR = "./dataserver_/mirror";
const char* DIR_FIRST_QUEUE = "./dataserver_/mirror/firstqueue";
const char* DIR_SECOND_QUEUE = "./dataserver_/mirror/secondqueue";
const char* FILE_FIRST_QUEUE_HEADER = "./dataserver_/mirror/firstqueue/header.dat";
const char* FILE_SECOND_QUEUE_HEADER = "./dataserver_/mirror/secondqueue/header.dat";
const char* FILE_SRC_BLOCK_FILE = "./dataserver_/mirror/firstqueue/src_block_file.dat";
const char* FILE_DEST_BLOCK_FILE = "./dataserver_/mirror/firstqueue/dest_block_file.dat";
const char* DIR_DISK = "./dataserver_/disk";
const char* DIR_DISK_INDEX = "./dataserver_/disk/index";
const char* DIR_TMP = "./dataserver_/tmp";

const int32_t WRITE_BUFF_SIZE = 1048576;

class SyncMirrorTest: public ::testing::Test
{
  public:
    SyncMirrorTest()
    {
    }

    ~SyncMirrorTest()
    {
    }

    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
  protected:
};

void generate_data(char *data, int32_t len)
{
  srand(time(NULL) + rand() + pthread_self());
  for (int i = 0; i < len; i++)
  {
    data[i] = rand() % 90 + 32;
  }
}

void write_src_block_file(const char* file_name, int32_t file_size)
{
  // generate data for write
  char data[WRITE_BUFF_SIZE];
  generate_data(data, WRITE_BUFF_SIZE);

  int32_t src_fd = -1;
  src_fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  EXPECT_GT(src_fd, 0);
  int64_t len_written = 0;
  int64_t len_to_write = 0;
  while (len_written < file_size)
  {
    len_to_write = (file_size - len_written) > WRITE_BUFF_SIZE ? WRITE_BUFF_SIZE : (file_size - len_written);
    len_written += write(src_fd, data, len_to_write);
  }
  EXPECT_EQ(len_written, file_size);
  close(src_fd);
}

/*TEST_F(SyncMirrorTest, testSyncBaseInit)
{
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->init();

  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  sync_base->stop();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testRemoteCopyFile)
{
  // write src_block_file : 100KB
  int64_t block_file_size = 102400;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;

  g_data_service.sync_mirror_.push_back(sync_base);
  sync_base->init();

  // write sync log
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 0;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_ERROR, ret);
  block_id = 101;
  ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check dest_block_file's size
  struct stat file_stat;
  stat(FILE_DEST_BLOCK_FILE, &file_stat);
  EXPECT_EQ(file_stat.st_size, block_file_size);

  // write src_block_file : 2MB
  block_file_size = 2097152;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check dest_block_file's size
  stat(FILE_DEST_BLOCK_FILE, &file_stat);
  EXPECT_EQ(file_stat.st_size, block_file_size);

  // write src_block_file : 5MB
  block_file_size = 5242880;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check dest_block_file's size
  stat(FILE_DEST_BLOCK_FILE, &file_stat);
  EXPECT_EQ(file_stat.st_size, block_file_size);

  sync_base->stop();
  g_data_service.sync_mirror_.clear();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testCopyFile)
{
  // init physical block
  int32_t physical_block_id = 1;
  int32_t block_length = 2 * 1024 * 1024;
  char file_name[100];
  mkdir(DIR_DISK, 0775);
  mkdir(DIR_DISK_INDEX, 0775);
  mkdir(DIR_TMP, 0775);
  sprintf(file_name, "%s/%d", DIR_DISK, physical_block_id);
  int32_t physical_block_fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, DIR_DISK, block_length, block_type);

  // init logic block
  uint32_t logic_block_id = 101;
  uint32_t  main_blk_key = 2;
  int32_t bucket_size = 16;
  MMapOption mmap_option;
  mmap_option.max_mmap_size_ = 256;
  mmap_option.first_mmap_size_ = 128;
  mmap_option.per_mmap_size_ = 4;

  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, DIR_DISK);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block);

  BlockFileManager::get_instance()->logic_blocks_.insert(BlockFileManager::LogicBlockMap::value_type(logic_block_id, logic_block));

  // generate data for write
  int64_t data_len = 1 * 1024 * 1024;
  char *write_data = new char[data_len];

  srand(time(NULL) + rand() + pthread_self());
  for (int32_t i = 0; i < data_len; i++)
  {
    write_data[i] = rand() % 90 + 32;
  }

  uint64_t file_number = 1;
  DataFile data_file(file_number, const_cast<char*>(DIR_DISK));
  data_file.set_data(write_data, data_len, 0);

  uint64_t inner_file_id = 1;
  uint32_t crc = 0;
  crc = Func::crc(crc, write_data, data_len);
  int32_t read_data_len = data_len + sizeof(FileInfo);
  char* read_file_data = new char[read_data_len];

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);
  EXPECT_EQ(logic_block->read_file(inner_file_id, read_file_data, read_data_len, 0, READ_DATA_OPTION_FLAG_NORMAL), 0);
  read_file_data[sizeof(FileInfo) + data_len] = '\0';
  EXPECT_STREQ(read_file_data + sizeof(FileInfo), write_data);

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;
  g_data_service.sync_mirror_.push_back(sync_base);

  sync_base->init();

  // write sync log
  int32_t cmd = OPLOG_INSERT;
  int32_t ret = sync_base->write_sync_log(cmd, logic_block_id, inner_file_id, inner_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check dest_block_file's size
  struct stat file_stat;
  stat(FILE_DEST_BLOCK_FILE, &file_stat);
  EXPECT_EQ(file_stat.st_size, data_len);

  sync_base->stop();

  delete write_data;
  write_data = NULL;
  delete read_file_data;
  read_file_data = NULL;

  // unlink file & rmdir
  logic_block->delete_block_file();
  sprintf(file_name, "%s/%d", DIR_DISK, physical_block_id);
  unlink(file_name);
  rmdir(DIR_DISK_INDEX);
  rmdir(DIR_TMP);
  rmdir(DIR_DISK);

  // close physical block file
  close(physical_block_fd);

  BlockFileManager::get_instance()->logic_blocks_.clear();
  g_data_service.sync_mirror_.clear();

  delete physical_block;
  physical_block = NULL;
  delete logic_block;
  logic_block = NULL;

  delete sync_base;
}

TEST_F(SyncMirrorTest, testRemoveFile)
{
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;
  g_data_service.sync_mirror_.push_back(sync_base);

  sync_base->init();

  // write src_block_file : 1MB
  int64_t block_file_size = 1048576;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // write sync log: INSERT
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // write sync log: REMOVE
  cmd = OPLOG_REMOVE;
  ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check if dest_block_file exist
  EXPECT_EQ(-1, access(FILE_DEST_BLOCK_FILE, 0));

  g_data_service.sync_mirror_.clear();
  sync_base->stop();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testRecoverSecondQueue)
{
  // write src_block_file : 1MB
  int64_t block_file_size = 1048576;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // construct the second file queue
  FileQueue* second_file_queue = new FileQueue(DIR_MIRROR , "secondqueue");
  second_file_queue->load_queue_head();
  second_file_queue->initialize();
  EXPECT_EQ(0, access(DIR_SECOND_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_SECOND_QUEUE_HEADER, 0));

  // push QueueItems into the second file queue
  SyncData sync_data;
  for (int32_t i = 0; i < 20; i++)
  {
    memset(&sync_data, 0, sizeof(SyncData));
    sync_data.cmd_ = OPLOG_INSERT;
    sync_data.block_id_ = i;
    sync_data.file_id_ = 1;
    sync_data.old_file_id_ = 1;
    sync_data.retry_time_ = time(NULL);
    second_file_queue->push(reinterpret_cast<void*>(&sync_data), sizeof(SyncData));
  }
  EXPECT_EQ(TFS_SUCCESS, second_file_queue->write_header());

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;
  g_data_service.sync_mirror_.push_back(sync_base);

  sync_base->init();
  EXPECT_GT(sync_base->file_queue_->queue_information_header_.queue_size_, 10);
  EXPECT_EQ(-1, access(DIR_SECOND_QUEUE, 0));

  // write sync log: INSERT
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // write sync log: REMOVE
  cmd = OPLOG_REMOVE;
  ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  // check if dest_block_file exist
  EXPECT_EQ(-1, access(FILE_DEST_BLOCK_FILE, 0));

  g_data_service.sync_mirror_.clear();
  sync_base->stop();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testSyncToMultiSlaves)
{
  SyncBase* sync_base = NULL;
  char queue_path[256];
  std::string queue_header_path = "";
  std::string file_src_block_file = "";
  std::string file_dest_block_file = "";
  int64_t block_file_size = 0;

  std::vector<std::string> slave_ns_ip;
  slave_ns_ip.push_back("10.232.36.205:8700");
  slave_ns_ip.push_back("10.232.36.208:8800");
  slave_ns_ip.push_back("10.232.36.209:8900");

  // init 3 SyncBase
  for (int32_t i = 0; i < 3; i++)
  {
    sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, i, "10.232.36.205:3100", slave_ns_ip.at(i).c_str());
    if (i == 0)
    {
      sprintf(queue_path, "%s", DIR_FIRST_QUEUE);
      queue_header_path = FILE_FIRST_QUEUE_HEADER;
    }
    else
    {
      sprintf(queue_path, "%s/queue_%"PRI64_PREFIX"u", DIR_MIRROR, Func::get_host_ip(slave_ns_ip.at(i).c_str()));
      queue_header_path = queue_path;
      queue_header_path += "/header.dat";
    }
    EXPECT_EQ(0, access(queue_path, 0));
    EXPECT_EQ(0, access(queue_header_path.c_str(), 0));

    file_src_block_file = queue_path;
    file_src_block_file += "/src_block_file.dat";
    file_dest_block_file = queue_path;
    file_dest_block_file += "/dest_block_file.dat";

    // write src_block_file : 1MB
    block_file_size = 1048576;
    write_src_block_file(file_src_block_file.c_str(), block_file_size);

    sync_base->src_block_file_ = file_src_block_file;
    sync_base->dest_block_file_ = file_dest_block_file;

    g_data_service.sync_mirror_.push_back(sync_base);
    sync_base->init();
  }

  // write sync log(master SyncBase)
  sync_base = g_data_service.sync_mirror_.at(0);
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // wait until filequeue empty
  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  for (int32_t i = 0; i < 3; i++)
  {
    if (i == 0)
    {
      sprintf(queue_path, "%s", DIR_FIRST_QUEUE);
    }
    else
    {
      sprintf(queue_path, "%s/queue_%d", DIR_MIRROR, i);
    }
    file_dest_block_file = queue_path;
    file_dest_block_file += "/dest_block_file.dat";
    // check dest_block_file's size
    struct stat file_stat;
    stat(file_dest_block_file.c_str(), &file_stat);
    EXPECT_EQ(file_stat.st_size, block_file_size);
  }

  std::vector<SyncBase*>::iterator iter = g_data_service.sync_mirror_.begin();
  for (; iter != g_data_service.sync_mirror_.end(); iter++)
  {
    (*iter)->stop();
    tbsys::gDelete((*iter));
  }
  g_data_service.sync_mirror_.clear();
}

TEST_F(SyncMirrorTest, testResetFileQueue)
{
  // write src_block_file : 100KB
  int64_t block_file_size = 102400;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;

  g_data_service.sync_mirror_.push_back(sync_base);
  sync_base->init();

  // write sync log
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  for (int32_t i = 0; i < 20; i++)
  {
    ret = sync_base->write_sync_log(cmd, 100 + i, file_id, old_file_id);
    EXPECT_EQ(TFS_SUCCESS, ret);
  }

  EXPECT_GT(sync_base->file_queue_->queue_information_header_.queue_size_, 10);

  // reset file queue
  EXPECT_EQ(TFS_SUCCESS, sync_base->reset_log());
  EXPECT_TRUE(sync_base->file_queue_->empty());

  // re-push QueuqItems
  for (int32_t i = 0; i < 5; i++)
  {
    ret = sync_base->write_sync_log(cmd, 100 + i, file_id, old_file_id);
    EXPECT_EQ(TFS_SUCCESS, ret);
  }

  EXPECT_GT(sync_base->file_queue_->queue_information_header_.queue_size_, 1);

  while (!sync_base->file_queue_->empty())
  {
    sleep(3);
  }

  sync_base->stop();
  g_data_service.sync_mirror_.clear();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testDisableFileQueue)
{
  // write src_block_file : 100KB
  int64_t block_file_size = 102400;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;

  g_data_service.sync_mirror_.push_back(sync_base);
  sync_base->init();

  // write sync log
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  for (int32_t i = 0; i < 20; i++)
  {
    ret = sync_base->write_sync_log(cmd, 100 + i, file_id, old_file_id);
    EXPECT_EQ(TFS_SUCCESS, ret);
  }

  EXPECT_GT(sync_base->file_queue_->queue_information_header_.queue_size_, 10);

  // disable file queue
  EXPECT_EQ(TFS_SUCCESS, sync_base->disable_log());
  EXPECT_TRUE(sync_base->file_queue_->empty());

  // re-push QueuqItems
  for (int32_t i = 0; i < 5; i++)
  {
    ret = sync_base->write_sync_log(cmd, 100 + i, file_id, old_file_id);
    EXPECT_EQ(TFS_SUCCESS, ret);
  }

  EXPECT_TRUE(sync_base->file_queue_->empty());

  sync_base->stop();
  g_data_service.sync_mirror_.clear();
  delete sync_base;
}

TEST_F(SyncMirrorTest, testSetPause)
{
  // write src_block_file : 100KB
  int64_t block_file_size = 102400;
  write_src_block_file(FILE_SRC_BLOCK_FILE, block_file_size);

  // init SyncBase
  SyncBase* sync_base = new SyncBase(g_data_service, SYNC_TO_TFS_MIRROR, 0, "xx.xx.xx.xx:3200", "xxx.xxx.xxx.xxx:3200");
  EXPECT_EQ(0, access(DIR_FIRST_QUEUE, 0));
  EXPECT_EQ(0, access(FILE_FIRST_QUEUE_HEADER, 0));

  sync_base->src_block_file_ = FILE_SRC_BLOCK_FILE;
  sync_base->dest_block_file_ = FILE_DEST_BLOCK_FILE;

  g_data_service.sync_mirror_.push_back(sync_base);
  sync_base->init();

  // write sync log
  int32_t cmd = OPLOG_INSERT;
  int32_t block_id = 101;
  int32_t file_id = 1;
  int32_t old_file_id = 1;
  int32_t ret = sync_base->write_sync_log(cmd, block_id, file_id, old_file_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  for (int32_t i = 0; i < 20; i++)
  {
    ret = sync_base->write_sync_log(cmd, 100 + i, file_id, old_file_id);
    EXPECT_EQ(TFS_SUCCESS, ret);
  }

  EXPECT_GT(sync_base->file_queue_->queue_information_header_.queue_size_, 10);

  // pause the sync
  sync_base->set_pause(1);

  // queue_size should not change
  sleep(1);
  int32_t queue_size = sync_base->file_queue_->queue_information_header_.queue_size_;
  for (int32_t i = 0; i < 5; i++)
  {
    EXPECT_EQ(sync_base->file_queue_->queue_information_header_.queue_size_, queue_size);
    sleep(1);
  }

  // resume the sync
  sync_base->set_pause(0);

  while (!sync_base->file_queue_->empty())
  {
    sleep(1);
  }

  sync_base->stop();
  g_data_service.sync_mirror_.clear();
  delete sync_base;
}*/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
