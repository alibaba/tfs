/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *    daoan(daoan@taobao.com)
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "define.h"
#include "meta_server_service.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;

class MetaInfoTest: public ::testing::Test
{
  public:
    MetaInfoTest()
    {
    }
    ~MetaInfoTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};
TEST_F(MetaInfoTest, FragMeta)
{
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 2;
  fm.offset_ = 1L<<60;
  fm.size_ = 500;
  char buf[100];
  int64_t pos = 0;
  fm.serialize(buf, 100, pos);
  int32_t data_len = pos;
  EXPECT_EQ(data_len, fm.get_length());
  memset(&fm, 1, sizeof(FragMeta));
  pos = 0;
  fm.deserialize(buf, data_len, pos);
  EXPECT_EQ(1, fm.block_id_);
  EXPECT_EQ(2, fm.file_id_);
  EXPECT_EQ(1L<<60, fm.offset_);
  EXPECT_EQ(500, fm.size_);
}
TEST_F(MetaInfoTest, FragInfo)
{
  FragInfo fi;
  fi.cluster_id_ = 5;

  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 2;
  fm.offset_ = 0;
  fm.size_ = 500;
  fi.v_frag_meta_.push_back(fm);
  fm.file_id_ = 3;
  fm.offset_ = 500;
  fm.size_ = 20;
  fi.v_frag_meta_.push_back(fm);
  EXPECT_EQ(520, fi.get_last_offset());

  char buf[1000];
  int64_t pos = 0;
  fi.serialize(buf, 1000, pos);
  int32_t data_len = pos;
  EXPECT_EQ(data_len, fi.get_length());
  FragInfo fi2;
  pos = 0;
  fi2.deserialize(buf, data_len, pos);
  EXPECT_EQ(5, fi2.cluster_id_);
  EXPECT_EQ(2, fi2.v_frag_meta_.size());
  EXPECT_EQ(500, fi2.v_frag_meta_[1].offset_);
  EXPECT_EQ(520, fi2.get_last_offset());
}
TEST_F(MetaInfoTest, FileMetaInfo)
{
  FileMetaInfo fmi;
  fmi.pid_ = 30;
  fmi.id_ = 40;
  fmi.create_time_ = 50;
  fmi.modify_time_ = 60;
  fmi.size_ = 70;
  fmi.ver_no_ = 80;
  fmi.name_ = "hello";
  Stream out;
  fmi.serialize(out);
  EXPECT_EQ(out.get_data_length(), fmi.length());
  FileMetaInfo fmi2;
  fmi2.deserialize(out);
  EXPECT_EQ(fmi.pid_, fmi2.pid_);
}
TEST_F(MetaInfoTest, MetaInfo)
{
  FileMetaInfo fmi;
  fmi.pid_ = 30;
  fmi.id_ = 40;
  fmi.create_time_ = 50;
  fmi.modify_time_ = 60;
  fmi.size_ = 70;
  fmi.ver_no_ = 80;
  fmi.name_ = "hello";
  Stream out;
  fmi.serialize(out);
  EXPECT_EQ(out.get_data_length(), fmi.length());
  FileMetaInfo fmi2;
  fmi2.deserialize(out);
  EXPECT_EQ(fmi.pid_, fmi2.pid_);
}
TEST_F(MetaInfoTest, check_frag_info_ok)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = false;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 20;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  
  fm.offset_ = 30;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_EQ(TFS_SUCCESS, MetaServerService::check_frag_info(fi));
}

TEST_F(MetaInfoTest, check_frag_info_overerite)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 0;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  //0 - 10
  
  fm.offset_ = 8;
  fm.size_ = 2;
  fi.v_frag_meta_.push_back(fm);  //8 - 10

  EXPECT_NE(TFS_SUCCESS, MetaServerService::check_frag_info(fi));
}
TEST_F(MetaInfoTest, check_frag_info_no_sort)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 8;
  fm.size_ = 2;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_NE(TFS_SUCCESS, MetaServerService::check_frag_info(fi));
}

TEST_F(MetaInfoTest, check_frag_info_error_cross)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 19;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_NE(TFS_SUCCESS, MetaServerService::check_frag_info(fi));
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
