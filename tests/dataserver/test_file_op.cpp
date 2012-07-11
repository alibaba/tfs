/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_file_op.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "file_op.h"

using namespace tfs::common;
const std::string FILE_NAME = "file_operation.test";

class FileOperationTest: public ::testing::Test
{
  public:
    FileOperationTest()
    {
      file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);
    }

    ~FileOperationTest()
    {
      if (file_op)
      {
        delete file_op;
        file_op = NULL;
      }
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
  protected:
    FileOperation* file_op;
};

TEST_F(FileOperationTest, testOpenappend)
{
  char* buf ="wuhan";
  char* buf1 ="beijing";

  FileOperation* file_op;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT | O_APPEND);

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);
  EXPECT_EQ(file_op->seek_file(0), 0);

  EXPECT_EQ(file_op->pwrite_file(buf, strlen(buf) + 1, 1), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf) + 1));

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);
  EXPECT_EQ(file_op->seek_file(0), 0);

  EXPECT_EQ(file_op->pwrite_file(buf1, strlen(buf1) + 1, 20), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf1) + 1));

  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testOpen)
{
  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);
  int32_t fd = file_op->open_file();
  EXPECT_GE(fd,0);
  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testWrite)
{
  char* buf ="hello";
  char* buf1 ="abc";

  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);

  EXPECT_EQ(file_op->write_file(buf, strlen(buf) + 1), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf) + 1));

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);
  EXPECT_EQ(file_op->seek_file(0), 0);

  EXPECT_EQ(file_op->write_file(buf1, strlen(buf1) + 1), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf1) + 1));

  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testPwrite)
{
  char* buf ="world";
  char* buf1 ="beijing";

  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);
  EXPECT_EQ(file_op->seek_file(0), 0);

  EXPECT_EQ(file_op->pwrite_file(buf, strlen(buf) + 1, 1), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf) + 2));

  file_op->ftruncate_file(0);
  EXPECT_EQ(file_op->get_file_size(), 0);
  EXPECT_EQ(file_op->seek_file(0), 0);

  EXPECT_EQ(file_op->pwrite_file(buf1, strlen(buf1) + 1, 0), 0);
  EXPECT_EQ(file_op->get_file_size(), static_cast<int64_t>(strlen(buf1) + 1));

  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testSeek)
{
  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);

  EXPECT_EQ(file_op->seek_file(1), 1);
  EXPECT_EQ(file_op->seek_file(2), 2);
  EXPECT_EQ(file_op->seek_file(0), 0);

  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testRead)
{
  char buf[100];
  int32_t size = 0;

  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);

  EXPECT_LT(file_op->pread_file(buf, 100, 0), 0);

  size = file_op->get_file_size();
  char* buf1 = new char[size];

  EXPECT_EQ(file_op->pread_file(buf, size, 0), 0);
  EXPECT_EQ(static_cast<int64_t>(strlen(buf) + 1), file_op->get_file_size());

  EXPECT_EQ(file_op->pread_file(buf1, size - 1, 1), 0);
  EXPECT_EQ(static_cast<int64_t>(strlen(buf1) + 1), file_op->get_file_size() - 1);

  delete buf1;
  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testFlushfile)
{
  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);
  EXPECT_EQ(file_op->flush_file(), 0);
  delete file_op;
  file_op = NULL;
}

TEST_F(FileOperationTest, testUnlink)
{
  FileOperation* file_op = NULL;
  file_op = new FileOperation(FILE_NAME, O_RDWR | O_LARGEFILE | O_CREAT);
  EXPECT_EQ(file_op->unlink_file(), 0);
  delete file_op;
  file_op = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
