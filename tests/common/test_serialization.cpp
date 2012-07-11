/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "serialization.h"

using namespace tfs::common;
    #pragma pack(4)
    struct TfsPacketNewHeaderV3
    {
      uint32_t flag_;
      int32_t length_;
      int16_t type_;
      int16_t check_;
    };

    struct TfsPacketNewHeaderV4
    {
      uint64_t id_;
      uint32_t flag_;
      uint32_t crc_;
      int32_t  length_;
      int16_t  type_;
      int16_t  version_;
    };
  struct TfsPacketNewHeaderV5
  {
    uint32_t flag_;
    int32_t length_;
    short type_;
    short check_;
  };
  
  struct TfsPacketNewHeaderV6
  {
    uint32_t flag_;
    int32_t length_;
    short type_;
    short version_;
    uint64_t id_;
    uint32_t crc_;
  };
  #pragma pack()
 
 

class TestSerialization : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }
  TestSerialization(){}
  ~TestSerialization(){}
};

static const int64_t BUF_LEN = 32;

TEST_F(TestSerialization, set_int8)
{
  int8_t value = 0xf;
  int64_t pos = 0;
  //data is null
  int32_t iret = Serialization::set_int8(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_int8(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_int8(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_int8(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_int8(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  //data_len < actual data length
  pos = 0;
  iret = Serialization::set_int8(data, 12, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT8_SIZE, pos);

  iret = Serialization::set_int8(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT8_SIZE * 2, pos);
  const int32_t count = (BUF_LEN - pos) / INT8_SIZE;
  for (int32_t i = 0; i < count; ++i)
  {
    iret = Serialization::set_int8(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(((i + 3) * INT8_SIZE), pos);
  }
  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, get_int8)
{
  char data[BUF_LEN];
  int8_t value = 0xf;
  int64_t pos = 0;
  int32_t iret = Serialization::set_int8(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT8_SIZE, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::get_int8(NULL, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  pos = 0;
  value = 0;
  iret = Serialization::get_int8(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0xf, value);
  EXPECT_EQ(INT8_SIZE, pos);

  const int32_t count = (BUF_LEN - pos) / INT8_SIZE;
  int32_t i = 0;
  for (i = 0; i < count; ++i)
  {
    iret = Serialization::set_int8(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xf, value);
    EXPECT_EQ((i + 2) * INT8_SIZE, pos);
  } 
  pos = 0;
  for (i = 0; i < count + 1; i++)
  {
    iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xf, value);
    EXPECT_EQ((i + 1) * INT8_SIZE, pos);
  }
  iret = Serialization::get_int8(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, set_int16)
{
  int16_t value = 0xff;
  int64_t pos = 0;
  //data is null
  int32_t iret = Serialization::set_int16(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_int16(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  //data_len < actual data length
  pos = 0;
  iret = Serialization::set_int16(data, 12, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT16_SIZE, pos);

  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT16_SIZE * 2, pos);
  const int32_t count = (BUF_LEN - pos) / INT16_SIZE;
  for (int32_t i = 0; i < count; ++i)
  {
    iret = Serialization::set_int16(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(((i + 3) * INT16_SIZE), pos);
  }
  iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, get_int16)
{
  char data[BUF_LEN];
  int16_t value = 0xff;
  int64_t pos = 0;
  int32_t iret = Serialization::set_int16(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT16_SIZE, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::get_int16(NULL, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  pos = 0;
  value = 0;
  iret = Serialization::get_int16(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0xff, value);
  EXPECT_EQ(INT16_SIZE, pos);

  const int32_t count = (BUF_LEN - pos) / INT16_SIZE;
  int32_t i = 0;
  for (i = 0; i < count; ++i)
  {
    iret = Serialization::set_int16(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xff, value);
    EXPECT_EQ((i + 2) * INT16_SIZE, pos);
  } 
  pos = 0;
  for (i = 0; i < count + 1; i++)
  {
    iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xff, value);
    EXPECT_EQ((i + 1) * INT16_SIZE, pos);
  }
  iret = Serialization::get_int16(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, set_int32)
{
  int32_t value = 0xffff;
  int64_t pos = 0;
  //data is null
  int32_t iret = Serialization::set_int32(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_int32(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  //data_len < actual data length
  pos = 0;
  iret = Serialization::set_int32(data, 12, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE, pos);

  iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE * 2, pos);
  const int32_t count = (BUF_LEN - pos) / INT_SIZE;
  for (int32_t i = 0; i < count; ++i)
  {
    iret = Serialization::set_int32(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(((i + 3) * INT_SIZE), pos);
  }
  iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, get_int32)
{
  char data[BUF_LEN];
  int32_t value = 0xffff;
  int64_t pos = 0;
  int32_t iret = Serialization::set_int32(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::get_int32(NULL, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  pos = 0;
  value = 0;
  iret = Serialization::get_int32(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0xffff, value);
  EXPECT_EQ(INT_SIZE, pos);

  const int32_t count = (BUF_LEN - pos) / INT_SIZE;
  int32_t i = 0;
  for (i = 0; i < count; ++i)
  {
    iret = Serialization::set_int32(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xffff, value);
    EXPECT_EQ((i + 2) * INT_SIZE, pos);
  } 
  pos = 0;
  for (i = 0; i < count + 1; i++)
  {
    iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xffff, value);
    EXPECT_EQ((i + 1) * INT_SIZE, pos);
  }
  iret = Serialization::get_int32(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, set_int64)
{
  int64_t value = 0xfffffffff;
  int64_t pos = 0;
  //data is null
  int32_t iret = Serialization::set_int64(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_int64(NULL, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  //data_len < actual data length
  pos = 0;
  iret = Serialization::set_int64(data, 12, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT64_SIZE, pos);

  iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT64_SIZE * 2, pos);
  const int32_t count = (BUF_LEN - pos) / INT64_SIZE;
  for (int32_t i = 0; i < count; ++i)
  {
    iret = Serialization::set_int64(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(((i + 3) * INT64_SIZE), pos);
  }
  iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, get_int64)
{
  char data[BUF_LEN];
  int64_t value = 0xfffffffff;
  int64_t pos = 0;
  int32_t iret = Serialization::set_int64(data, BUF_LEN, pos, value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT64_SIZE, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::get_int64(NULL, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  pos = 0;
  value = 0;
  iret = Serialization::get_int64(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0xfffffffff, value);
  EXPECT_EQ(INT64_SIZE, pos);

  const int32_t count = (BUF_LEN - pos) / INT64_SIZE;
  int32_t i = 0;
  for (i = 0; i < count; ++i)
  {
    iret = Serialization::set_int64(data, BUF_LEN, pos, value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xfffffffff, value);
    EXPECT_EQ((i + 2) * INT64_SIZE, pos);
  } 
  pos = 0;
  for (i = 0; i < count + 1; i++)
  {
    iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
    EXPECT_EQ(TFS_SUCCESS, iret);
    EXPECT_EQ(0xfffffffff, value);
    EXPECT_EQ((i + 1) * INT64_SIZE, pos);
  }
  iret = Serialization::get_int64(data, BUF_LEN, pos, &value);
  EXPECT_EQ(TFS_ERROR, iret);
}

#pragma pack(4)
struct TestStructCompatible
{
  int32_t value1_;
  int64_t value2_;
  int32_t value3_;
  int32_t value4_;
  int64_t value5_;
  int deserialize(const char* data, const int64_t data_len, int64_t& pos);
};
#pragma pack()

int TestStructCompatible:: deserialize(const char* data, const int64_t data_len, int64_t& pos)
{
  int32_t iret = NULL != data && data_len - pos >= static_cast<int64_t>(sizeof(TestStructCompatible)) ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    iret = Serialization::get_int32(data, BUF_LEN, pos, &value1_);
    if (TFS_SUCCESS == iret)
    {
      iret = Serialization::get_int64(data, BUF_LEN, pos, &value2_);
    }
    if (TFS_SUCCESS == iret)
    {
      iret = Serialization::get_int32(data, BUF_LEN, pos, &value3_);
    }
    if (TFS_SUCCESS == iret)
    {
      iret = Serialization::get_int32(data, BUF_LEN, pos, &value4_);
    }
    if (TFS_SUCCESS == iret)
    {
      iret = Serialization::get_int64(data, BUF_LEN, pos, &value5_);
    }
  }
  return iret;
}

TEST_F(TestSerialization, test_struct_compatible)
{
  const int32_t LEN = sizeof(TestStructCompatible);
  char data[LEN];

  TestStructCompatible value;
  memset(&value, 0, sizeof(value));

  value.value1_ = 0xffff + 1;
  value.value2_ = 0xffff + 2;
  value.value3_ = 0xffff + 3;
  value.value4_ = 0xffff + 4;
  value.value5_ = 0xffff + 5;
  memcpy(data, &value, sizeof(value));

  TestStructCompatible tmp;
  memset(&tmp, 0, sizeof(tmp));
  int64_t pos = 0;
  int32_t iret = tmp.deserialize(data, LEN, pos);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0xffff + 1, tmp.value1_);
  EXPECT_EQ(0xffff + 2, tmp.value2_);
  EXPECT_EQ(0xffff + 3, tmp.value3_);
  EXPECT_EQ(0xffff + 4, tmp.value4_);
  EXPECT_EQ(0xffff + 5, tmp.value5_);
};

TEST_F(TestSerialization, test_set_string)
{
  int64_t pos = 0;
  std::string value("test");
  //data is null
  int32_t iret = Serialization::set_string(NULL, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_string(NULL, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_string(data, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_string(data, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_string(data, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  // value is null
  pos = 0;
  iret = Serialization::set_string(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE, pos);

  //data_len < actual data length
  pos = 0;
  iret = Serialization::set_string(data, 12, pos, value.c_str());
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(static_cast<int64_t>((value.length() + INT_SIZE + 1)), pos);
  const int32_t LEN = BUF_LEN - value.length() - INT_SIZE - INT_SIZE - 2;
  std::string tmp(LEN, 'a');
  iret = Serialization::set_string(data, BUF_LEN, pos, tmp.c_str());
  EXPECT_EQ(TFS_SUCCESS, iret);

  iret = Serialization::set_string(data, BUF_LEN, pos, tmp.c_str());
  EXPECT_EQ(TFS_ERROR, iret);
}

TEST_F(TestSerialization, test_get_string)
{
  int64_t pos = 0;
  std::string value("test");
  //data is null
  char data[BUF_LEN];
  int32_t iret = Serialization::set_string(data, BUF_LEN, pos, value.c_str());
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(static_cast<int64_t>((value.length() + INT_SIZE + 1)), pos);

  char str[BUF_LEN];

  //data is null && pos < 0
  pos = -1;
  int64_t str_len = BUF_LEN;
  iret = Serialization::get_string(NULL, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  str_len = BUF_LEN;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  str_len = BUF_LEN;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(str_len, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  str_len = BUF_LEN;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  pos = 0;
  str_len = value.length() - 1;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  str_len = BUF_LEN;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0, memcmp(value.c_str(), str, value.length()));

  pos = 0;
  iret = Serialization::set_string(data, BUF_LEN, pos, NULL);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE, pos);

  pos = 0;
  str_len = BUF_LEN;
  iret = Serialization::get_string(data, BUF_LEN, pos, str, str_len);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(INT_SIZE, pos);
}
TEST_F(TestSerialization, test_string_set_get)
{
  char data[BUF_LEN];
  char data2[BUF_LEN];
  std::string t1 = "1234";
  std::string t2;
  int64_t pos;
  int iret;

  pos = 0;
  iret = Serialization::set_string(data, BUF_LEN, pos, t1);
  EXPECT_EQ(TFS_SUCCESS, iret);
  pos = 0;
  iret = Serialization::get_string(data, BUF_LEN, pos, t2);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_TRUE(t1 == t2);
  EXPECT_EQ(t1.length(), t2.length());

  
  data[0] = 0;
  data[1] = '2';
  std::string tmp(data, 2);
  data2[0] = 1;
  pos = 0;
  iret = Serialization::set_string(data, BUF_LEN, pos, data);
  EXPECT_EQ(TFS_SUCCESS, iret);
  pos = 0;
  int64_t buff_len = 1;
  iret = Serialization::get_string(data, BUF_LEN, pos, data2, buff_len);
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0 , buff_len);
  EXPECT_EQ(data[0], data2[0]);

}

TEST_F(TestSerialization, test_set_bytes)
{
  int64_t pos = 0;
  std::string value("test");
  //data is null
  int32_t iret = Serialization::set_bytes(NULL, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::set_bytes(NULL, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  char data[BUF_LEN];
  iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  // value is null
  pos = 0;
  iret = Serialization::set_bytes(data, BUF_LEN, pos, NULL, value.length());
  EXPECT_EQ(TFS_ERROR, iret);

  iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), 0);
  EXPECT_EQ(TFS_ERROR, iret);

  iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(static_cast<int64_t>(value.length()), pos);
}

TEST_F(TestSerialization, test_get_bytes)
{
  int64_t pos = 0;
  std::string value("test");
  char data[BUF_LEN];
  char buf[BUF_LEN];

  int32_t iret = Serialization::set_bytes(data, BUF_LEN, pos, value.c_str(), value.length());
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(static_cast<int64_t>(value.length()), pos);

  //data is null && pos < 0
  pos = -1;
  iret = Serialization::get_bytes(NULL, BUF_LEN, pos, buf, BUF_LEN);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos < 0
  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, BUF_LEN);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(-1, pos);

  //pos == data_len
  pos = BUF_LEN;
  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, BUF_LEN);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN, pos);

  //pos > data_len
  pos = BUF_LEN + 1;
  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, BUF_LEN);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(BUF_LEN + 1, pos);

  //buf is null
  pos = 0;
  iret = Serialization::get_bytes(data, BUF_LEN, pos, NULL, BUF_LEN);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, 0);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_bytes(data, BUF_LEN, pos, NULL, 0);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  // buf length > data length
  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, BUF_LEN + 1);
  EXPECT_EQ(TFS_ERROR, iret);
  EXPECT_EQ(0, pos);

  iret = Serialization::get_bytes(data, BUF_LEN, pos, buf, value.length());
  EXPECT_EQ(TFS_SUCCESS, iret);
  EXPECT_EQ(0, memcmp(value.c_str(), buf, value.length()));
  EXPECT_EQ(static_cast<int64_t>(value.length()), pos);
}

int main(int argc, char* argv[])
{
  printf("TfsPacketNewHeaderV3: %d\n", sizeof(TfsPacketNewHeaderV3));
  printf("TfsPacketNewHeaderV4: %d\n", sizeof(TfsPacketNewHeaderV4));
  printf("TfsPacketNewHeaderV5: %d\n", sizeof(TfsPacketNewHeaderV5));
  printf("TfsPacketNewHeaderV6: %d\n", sizeof(TfsPacketNewHeaderV6));
 
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
