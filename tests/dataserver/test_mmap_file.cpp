/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_mmap_file.cpp 36 2010-11-02 08:14:59Z nayan@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "mmap_file.h"
#include <tbsys.h>

using namespace tfs::common;
const char* MMAP_FILE_NAME = "file_mmap.test";

class MMapFileTest: public ::testing::Test
{
  protected:
    static void SetUpTestCase()
    {
      TBSYS_LOGGER.setLogLevel("error");
    }

    static void TearDownTestCase()
    {
      TBSYS_LOGGER.setLogLevel("debug");
    }

  public:
    MMapFileTest()
    {
      mmap_option.max_mmap_size_ = 2560;
      mmap_option.first_mmap_size_ = 128;
      mmap_option.per_mmap_size_ = 4;
    }
    ~MMapFileTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
  protected:
    tfs::common::MMapOption mmap_option;
};

TEST_F(MMapFileTest, testMapFile)
{
  int32_t fd = 0;
  int32_t file_size = 0;

  fd = open(MMAP_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  EXPECT_LE(0, fd);

  MMapFile* mmap_file = NULL;
  mmap_file = new MMapFile(mmap_option, fd);

  EXPECT_EQ(mmap_file->map_file(), 1);

  struct stat statbuf;
  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;
  EXPECT_EQ(file_size, static_cast<int32_t>(mmap_option.first_mmap_size_));

  EXPECT_EQ(mmap_file->remap_file(), 1);

  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;
  EXPECT_EQ(file_size, static_cast<int32_t>(mmap_option.first_mmap_size_ + mmap_option.per_mmap_size_));

  close(fd);
  fd = 0;
  unlink(MMAP_FILE_NAME);
  delete mmap_file;
  mmap_file = NULL;
}

TEST_F(MMapFileTest, testRemapFile)
{
  int32_t fd = 0;
  int32_t file_size = 0;
  int32_t file_remap_size = 0;
  int32_t total_num = 3300;

  fd = open(MMAP_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  EXPECT_LE(0, fd);

  MMapFile* mmap_file = NULL;
  mmap_file = new MMapFile(mmap_option, fd);

  EXPECT_EQ(mmap_file->map_file(), 1);

  struct stat statbuf;
  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;
  EXPECT_EQ(file_size, static_cast<int32_t>(mmap_option.first_mmap_size_));

  for (int32_t i = 1; i <= total_num; ++i)
  {
    file_remap_size = static_cast<int32_t>(mmap_option.first_mmap_size_ + i * mmap_option.per_mmap_size_);

    if(file_size == static_cast<int32_t>(mmap_option.max_mmap_size_))
    {
      file_remap_size = static_cast<int32_t>(mmap_option.max_mmap_size_);
      EXPECT_EQ(mmap_file->remap_file(), 0);
    }
    else if(file_remap_size > static_cast<int32_t>(mmap_option.max_mmap_size_))
    {
      file_remap_size = static_cast<int32_t>(mmap_option.max_mmap_size_);
      EXPECT_EQ(mmap_file->remap_file(), 1);
    }
    else
    {
      EXPECT_EQ(mmap_file->remap_file(), 1);
    }
    stat(MMAP_FILE_NAME, &statbuf);
    file_size = statbuf.st_size;

    EXPECT_EQ(file_size, file_remap_size);
  }

  close(fd);
  fd = 0;
  unlink(MMAP_FILE_NAME);
  delete mmap_file;
  mmap_file = NULL;
}

TEST_F(MMapFileTest, testMapFileData)
{
  int32_t fd = 0;
  int32_t file_size = 0;
  char* buf = NULL;

  fd = open(MMAP_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  EXPECT_LE(0, fd);

  MMapFile* mmap_file = NULL;
  mmap_file = new MMapFile(mmap_option, fd);

  EXPECT_EQ(mmap_file->map_file(), 1);

  struct stat statbuf;
  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;

  buf = new char[file_size];
  int32_t j = 0;
  for (j = 0; j < file_size; ++j)
  {
    buf[j] = 'a' + (j % 26);
  }

  write(fd, buf, file_size);

  if (mmap_file->get_data() == NULL)
  {
    ADD_FAILURE();
  }

  char* tmp_data = reinterpret_cast<char*>(mmap_file->get_data());

  for(int32_t i = 0; i < file_size; ++i )
  {
    EXPECT_EQ(buf[i], tmp_data[i]);
  }

  close(fd);
  fd = 0;
  delete []buf;
  buf = NULL;
  delete mmap_file;
  mmap_file = NULL;
}

TEST_F(MMapFileTest, testRemapFileData)
{
  int32_t fd = 0;
  int32_t file_size = 0;
  char* buf = NULL;

  fd = open(MMAP_FILE_NAME,O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  EXPECT_LE(0, fd);

  MMapFile* mmap_file = NULL;
  mmap_file = new MMapFile(mmap_option, fd);

  EXPECT_EQ(mmap_file->map_file(), 1);
  EXPECT_EQ(mmap_file->remap_file(), 1);

  struct stat statbuf;
  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;

  buf = new char[file_size];
  int32_t j = 0;
  for (j = 0; j < file_size; ++j)
  {
    buf[j] = 'a' + (j % 26);
  }

  write(fd ,buf , file_size);
  if (mmap_file->get_data() == NULL)
  {
    ADD_FAILURE();
  }

  char* tmp_data = reinterpret_cast<char*>(mmap_file->get_data());

  for(int32_t i = 0; i < file_size; ++i )
  {
    EXPECT_EQ(buf[i], tmp_data[i]);
  }

  unlink(MMAP_FILE_NAME);
  close(fd);
  fd = 0;
  delete []buf;
  buf = NULL;
  delete mmap_file;
  mmap_file = NULL;
}

TEST_F(MMapFileTest, testSyncFile)
{
  int32_t fd = 0;
  int32_t file_size = 0;

  fd = open(MMAP_FILE_NAME,O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  EXPECT_LE(0, fd);

  MMapFile* mmap_file = NULL;
  mmap_file = new MMapFile(mmap_option, fd);

  EXPECT_EQ(mmap_file->map_file(1), 1);//for write

  struct stat statbuf;
  stat(MMAP_FILE_NAME, &statbuf);
  file_size = statbuf.st_size;

  char* buf = (char *)mmap_file->get_data();
  for (int32_t i = 0; i < file_size; ++i)
  {
    buf[i] = 'a' + i%26;
  }

  EXPECT_TRUE(mmap_file->sync_file());

  char* tmp_data = new char[file_size];
  read(fd, tmp_data, file_size);

  for (int32_t i = 0; i< file_size; ++i)
  {
    EXPECT_EQ(buf[i], tmp_data[i]);
  }

  close(fd);
  fd = 0;
  unlink(MMAP_FILE_NAME);
  delete mmap_file;
  delete []tmp_data;
  mmap_file = NULL;

}

TEST_F(MMapFileTest, testMultiFile)
{
  int32_t fd = 0;
  int32_t file_size = 0;
  int32_t total_file_num = 40;
  char test_file_name[100];
  char* buf;

  MMapFile* mmap_file[total_file_num];

  mkdir("./data", 0775);
  for (int32_t i = 1; i <= total_file_num; i++)
  {
    sprintf(test_file_name, "./data/test_mmap_data_%d", i);
    fd = open(test_file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    EXPECT_LE(0, fd);

    mmap_file[i] = new MMapFile(mmap_option, fd);

    EXPECT_EQ(mmap_file[i]->map_file(), 1);

    struct stat statbuf;
    stat(test_file_name, &statbuf);
    file_size = statbuf.st_size;

    buf = new char[file_size];
    int32_t j = 0;
    for ( j = 0; j < file_size; ++j)
    {
      buf[j] = 'a' + (j % 26);
    }

    write(fd, buf, file_size);

    close(fd);
    fd = 0;

    EXPECT_EQ(file_size, static_cast<int32_t>(mmap_option.first_mmap_size_));
    delete []buf;
    buf = NULL;
  }

  for (int32_t i = 1; i <= total_file_num; ++i)
  {
    sprintf(test_file_name, "./data/test_mmap_data_%d", i);
    unlink(test_file_name);
    delete mmap_file[i];
    mmap_file[i] = NULL;
  }
  rmdir("./data");
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
