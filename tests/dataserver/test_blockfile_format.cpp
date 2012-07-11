/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_blockfile_format.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "blockfile_format.h"
using namespace tfs::dataserver;

const char* DIR_TEST = "./test";
class FileFormaterTest: public ::testing::Test
{
  public:
    FileFormaterTest()
    {
    }
    ~FileFormaterTest()
    {
    }
    virtual void SetUp()
    {
      mkdir(DIR_TEST, 0775);
    }
    virtual void TearDown()
    {
      rmdir(DIR_TEST);
    }
};

TEST_F(FileFormaterTest, testFileFormatExt4Alloc)
{
  //TODO: FileFormatExt4Alloc test
}

TEST_F(FileFormaterTest, testFileFormatExt3Alloc)
{
  chdir(DIR_TEST);
  struct stat stat_buf;

  int32_t file_size = 100;
  int32_t fd = open("ext3_alloc_1", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  FileFormater* ptr1 = new Ext3FullFileFormater();
  EXPECT_EQ(ptr1 -> block_file_format(fd, file_size), 0);
  lstat("ext3_alloc_1", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 100);

  delete ptr1;
  ptr1 = NULL;
  close(fd);

  file_size = 1024;
  fd = open("ext3_alloc_2", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  FileFormater* ptr2 = new Ext3FullFileFormater();
  EXPECT_EQ(ptr2 -> block_file_format(fd, file_size), 0);
  lstat("ext3_alloc_2", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 1024);

  delete ptr2;
  ptr2 = NULL;
  close(fd);

  file_size = 2 * 1024 * 1024;
  fd = open("ext3_alloc_3", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  
  FileFormater* ptr3 = new Ext3FullFileFormater();
  EXPECT_EQ(ptr3 -> block_file_format(fd, file_size), 0);
  lstat("ext3_alloc_3", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 2 * 1024 * 1024);

  unlink("ext3_alloc_1");
  unlink("ext3_alloc_2");
  unlink("ext3_alloc_3");

  close(fd);
  delete ptr3;
  ptr3 = NULL;

  chdir("..");
}

TEST_F(FileFormaterTest, testFileFormatExt3NoAlloc)
{
  chdir(DIR_TEST);
  struct stat stat_buf;

  int32_t file_size = 100;
  int32_t fd = open("ext3_no_alloc_1", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  FileFormater* ptr1 = new Ext3SimpleFileFormater();
  EXPECT_EQ(ptr1 -> block_file_format(fd, file_size), 0);
  lstat("ext3_no_alloc_1", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 100);

  delete ptr1;
  ptr1 = NULL;
  close(fd);

  file_size = 1024;
  fd = open("ext3_no_alloc_2", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  FileFormater* ptr2 = new Ext3SimpleFileFormater();
  EXPECT_EQ(ptr2 -> block_file_format(fd, file_size), 0);
  lstat("ext3_no_alloc_2", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 1024);

  delete ptr2;
  ptr2 = NULL;
  close(fd);

  file_size = 2 * 1024 * 1024;
  fd = open("ext3_no_alloc_3", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  FileFormater* ptr3 = new Ext3SimpleFileFormater();
  EXPECT_EQ(ptr3 -> block_file_format(fd, file_size), 0);
  lstat("ext3_no_alloc_3", &stat_buf);
  EXPECT_EQ(stat_buf.st_size, 2 * 1024 * 1024);

  close(fd);
  delete ptr3;
  ptr3 = NULL;

  unlink("ext3_no_alloc_1");
  unlink("ext3_no_alloc_2");
  unlink("ext3_no_alloc_3");

  chdir("..");
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
