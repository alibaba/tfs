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
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "dataserver_define.h"
#include "common/define.h"

using namespace tfs::dataserver;
using namespace tfs::common;

class MetaTest: public ::testing::Test 
{
  public:
    MetaTest()
    {
    }

    ~MetaTest()
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

TEST_F(MetaTest, testRawMetaConstruct)
{  
  uint64_t file_id = 10023;
  int32_t offset = 232;
  int32_t size = 972;

  RawMeta raw_meta(file_id, offset, size);
  RawMeta raw_meta_copy(raw_meta);
  RawMeta raw_meta_assign = raw_meta;

  EXPECT_EQ(raw_meta.get_file_id(), file_id);
  EXPECT_EQ(raw_meta.get_offset(), offset);
  EXPECT_EQ(raw_meta.get_size(), size);

  EXPECT_EQ(raw_meta.get_file_id(), raw_meta_copy.get_file_id());
  EXPECT_EQ(raw_meta.get_offset(), raw_meta_copy.get_offset());
  EXPECT_EQ(raw_meta.get_size(), raw_meta_copy.get_size());

  EXPECT_EQ(raw_meta.get_file_id(), raw_meta_assign.get_file_id());
  EXPECT_EQ(raw_meta.get_offset(), raw_meta_assign.get_offset());
  EXPECT_EQ(raw_meta.get_size(), raw_meta_assign.get_size());

  EXPECT_TRUE(raw_meta_assign == raw_meta);
  EXPECT_TRUE(raw_meta_copy == raw_meta);
}

TEST_F(MetaTest, testMetaInfoConstruct)
{  
  uint64_t file_id = 10025;
  int32_t offset = 235;
  int32_t size = 975;

  RawMeta raw_meta(file_id, offset, size);

  EXPECT_EQ(raw_meta.get_file_id(), file_id);
  EXPECT_EQ(raw_meta.get_offset(), offset);
  EXPECT_EQ(raw_meta.get_size(), size);

  MetaInfo meta_info(raw_meta);
  EXPECT_EQ(meta_info.get_file_id(), raw_meta.get_file_id());
  EXPECT_EQ(meta_info.get_offset(), raw_meta.get_offset());
  EXPECT_EQ(meta_info.get_size(), raw_meta.get_size());
  EXPECT_EQ(meta_info.get_next_meta_offset(), 0);

  MetaInfo meta_info_copy(meta_info);
  EXPECT_EQ(meta_info.get_file_id(), meta_info_copy.get_file_id());
  EXPECT_EQ(meta_info.get_offset(), meta_info_copy.get_offset());
  EXPECT_EQ(meta_info.get_size(), meta_info_copy.get_size());
  EXPECT_EQ(meta_info.get_next_meta_offset(), meta_info_copy.get_next_meta_offset());

  MetaInfo meta_info_assign = meta_info;
  EXPECT_EQ(meta_info.get_file_id(), meta_info_assign.get_file_id());
  EXPECT_EQ(meta_info.get_offset(), meta_info_assign.get_offset());
  EXPECT_EQ(meta_info.get_size(), meta_info_assign.get_size());
  EXPECT_EQ(meta_info.get_next_meta_offset(), meta_info_assign.get_next_meta_offset());
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
