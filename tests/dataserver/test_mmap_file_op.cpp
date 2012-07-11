/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_mmap_file_op.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include "mmap_file_op.h"

using namespace tfs::dataserver;
const std::string FILE_NAME = "file_mmap_op.test";

class MMapFileOperationTest: public ::testing::Test 
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
    MMapFileOperationTest()
    {
      mmap_option.max_mmap_size_ = 256;
      mmap_option.first_mmap_size_ = 128;
      mmap_option.per_mmap_size_ = 4;
    }

    ~MMapFileOperationTest()
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

TEST_F(MMapFileOperationTest, testWriteRead) 
{
  char* buf = NULL;
  char* data = NULL;

  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();
  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  for (int32_t data_size = 128 ; data_size < 300; ++data_size)
  {
    buf = new char[data_size];
 
    for(int32_t i= 0; i < data_size - 1; ++i)
    { 
      buf[i] = 'a' + i % 26;
    } 
    buf[data_size - 1] = '\0';

    data = new char[data_size];
    EXPECT_EQ(mmap_file_op->pwrite_file(buf, data_size, 0), 0);
    EXPECT_EQ(mmap_file_op->pread_file(data, data_size, 0), 0); 

    for (int32_t j = 0; j< data_size - 1; ++j)
    {
      EXPECT_EQ(buf[j], data[j]);
    }
  
    delete []data;
    data = NULL;
    delete []buf;
    buf = NULL;
  }

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

TEST_F(MMapFileOperationTest, testWriteReadOffset) 
{ 
  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();
  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  int32_t data_size = 300;
  char* buf = new char[data_size];
 
  for (int32_t i= 0; i < data_size - 1; ++i)
  { 
    buf[i] = 'a' + i % 26;
  }
  buf[data_size - 1] = '\0';

  char* data = new char[data_size];

  for (int32_t offset = 0; offset < static_cast<int32_t>(mmap_option.max_mmap_size_); ++offset )
  {
    EXPECT_EQ(mmap_file_op->pwrite_file(buf, data_size, offset), 0);
    EXPECT_EQ(mmap_file_op->pread_file(data, data_size, offset), 0); 

    for (int32_t j = 0; j< data_size -1; ++j)
    {
      EXPECT_EQ(buf[j], data[j]);
    }
  }

  delete []data;
  data = NULL;
  delete []buf;
  buf = NULL;

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

TEST_F(MMapFileOperationTest, testGetMapData)
{
  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();

  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  int32_t data_size = 128;
  char* buf = new char[data_size];
 
  for (int32_t i = 0; i < data_size - 1; ++i)
  { 
    buf[i] = 'a' + i % 26;
  }
  buf[data_size - 1] = '\0';

  EXPECT_EQ(mmap_file_op->pwrite_file(buf, data_size, 0), 0);
  char* data = reinterpret_cast<char*>(mmap_file_op->get_map_data());

  for (int32_t j = 0; j < data_size - 1; ++j)
  {
    EXPECT_EQ(buf[j], data[j]);
  }
  
  EXPECT_EQ(mmap_file_op->munmap_file(), 0);

  delete []buf;
  buf = NULL;

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

TEST_F(MMapFileOperationTest, testMapSyncFile)
{
  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();

  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  int32_t data_size = 128;
  char* data = reinterpret_cast<char*>(mmap_file_op->get_map_data());

  for (int32_t i = 0; i < data_size - 1; ++i)
  { 
    data[i] = 'a' + i % 26;
  }

  EXPECT_EQ(mmap_file_op->flush_file(), 0);
  char* buf =new char[data_size];
  EXPECT_EQ(mmap_file_op->pread_file(buf, data_size, 0), 0);

  for (int32_t j = 0; j < data_size -1; ++j)
  {
    EXPECT_EQ(buf[j], data[j]);
  }
  
  EXPECT_EQ(mmap_file_op->munmap_file(), 0);

  delete []buf;
  buf = NULL;

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

TEST_F(MMapFileOperationTest, testReadForParaInfo)
{
  char* buf = NULL;
  char* data = NULL;

  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();
  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  for (int32_t data_size = 2; data_size < 300; ++data_size)
  {
    buf = new char[data_size];
 
    for (int32_t i= 0; i < data_size - 1; ++i)
    { 
      buf[i] = 'a' + i % 26;
    } 
    buf[data_size - 1] = '\0';

    ParaInfo para_info(data_size);
    EXPECT_EQ(mmap_file_op->pwrite_file(buf, data_size, 0), 0);
    EXPECT_EQ(mmap_file_op->pread_file(para_info, data_size, 0), 0);  
    data = para_info.get_actual_buf();

    for (int32_t j = 0; j< data_size -1; ++j)
    {
      EXPECT_EQ(buf[j], data[j]);
    }

    delete []buf;
    buf = NULL;
  }

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

TEST_F(MMapFileOperationTest, testReadParainfoOffset) 
{
  int32_t data_size = 300; 
  char* data = NULL;
  char* buf = new char[data_size];

  MMapFileOperation* mmap_file_op = NULL;
  mmap_file_op = new MMapFileOperation(FILE_NAME, O_RDWR | O_CREAT);
  mmap_file_op->open_file();
  EXPECT_EQ(mmap_file_op->mmap_file(mmap_option), 0);

  ParaInfo para_info(data_size);

  for (int32_t i= 0; i < data_size - 1; ++i)
  { 
    buf[i] = 'a' + i % 26;
  }
  buf[data_size - 1] = '\0';

  for (int32_t offset = 0; offset < static_cast<int32_t>(mmap_option.max_mmap_size_); ++offset )
  {
    EXPECT_EQ(mmap_file_op->pwrite_file(buf, data_size, offset), 0);
    EXPECT_EQ(mmap_file_op->pread_file(para_info, data_size, offset), 0); 
    data = para_info.get_actual_buf();

    for (int32_t j = 0; j< data_size -1; ++j)
    {
      EXPECT_EQ(buf[j], data[j]);
    }
  }

  delete []buf;
  buf = NULL;

  mmap_file_op->unlink_file();
  delete mmap_file_op;
  mmap_file_op = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
