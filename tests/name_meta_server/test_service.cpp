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
#include "meta_cache_info.h"
#include "meta_cache_helper.h"
#include "meta_server_service.h"
#include "common/parameter.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;
using namespace std;

class ServiceTest: public ::testing::Test
{
  public:
    ServiceTest()
    {
    }
    ~ServiceTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

const int32_t FRAG_LEN = 65535;
void dump_meta_info(const MetaInfo& metainfo)
{
  int size = metainfo.file_info_.size_;
  int nlen = metainfo.file_info_.name_.length();

  TBSYS_LOG(INFO, "size = %d, name_len = %d", size, nlen);
}
void dump_frag_meta(const FragMeta& fm)
{
  TBSYS_LOG(INFO, "offset_ %ld file_id_ %lu size_ %d block_id_ %u",
      fm.offset_, fm.file_id_, fm.size_, fm.block_id_);
}
tfs::namemetaserver::MetaServerService service;
NameMetaServerParameter::DbInfo dbinfo;
int64_t app_id = 19;
int64_t uid = 5;

TEST_F(ServiceTest, create_dir)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test1");
  sprintf(new_dir_path, "/test1");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  CacheRootNode* p = NULL;

  TBSYS_LOG(INFO, "create /test1/");
  sprintf(new_dir_path, "/test1/");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test1/ff/");
  sprintf(new_dir_path, "/test1/ff/");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  //p = service.get_root_node(app_id, uid);
  //p->dump();
  //service.revert_root_node(app_id, uid);
  //now we have  /test1/   /test1/ff/  /test2/
  //use this to test gc
  TBSYS_LOG(INFO, "create /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.create(app_id, uid + 1, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
}
TEST_F(ServiceTest, create_file)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test3/f1");
  sprintf(new_dir_path, "/test3/f1");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test1/ff/");
  sprintf(new_dir_path, "/test1/ff/");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  //now we have  /test1/   /test1/ff/  /test2/ff
}
TEST_F(ServiceTest, rm_dir)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "rm /test1/ff/f1");
  sprintf(new_dir_path, "/test1/ff/f1");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "rm /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "create /test2/");
  sprintf(new_dir_path, "/test2/");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  //now we have  /test1/    /test2/ff
}
TEST_F(ServiceTest, rm_file)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "rm /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "rm /test3/f1");
  sprintf(new_dir_path, "/test3/f1");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "rm /test1");
  sprintf(new_dir_path, "/test1");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "rm /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //now we have  /test1/   /test2/
} 
TEST_F(ServiceTest, mv_dir)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "mv /test2/ /test1/test3/t2");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1/test3/t2");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "mv /test2/ /test1");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "mv /test2/ /test1/test3");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1/test3");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  //now we have  /test1/test3/ff 
} 

TEST_F(ServiceTest, mv_file)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;


  TBSYS_LOG(INFO, "mv /test1/test3/ff /test2");
  sprintf(new_dir_path, "/test1/test3/ff");
  sprintf(dest_dir_path, "/test2");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);

  TBSYS_LOG(INFO, "mv /test1/test3 /test4");
  sprintf(new_dir_path, "/test1/test3");
  sprintf(dest_dir_path, "/test4");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);

  //now we have  /test1/test3/ /test2
} 

TEST_F(ServiceTest, write_file)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;

  FragInfo fra_info;
  fra_info.cluster_id_ = 1;
  FragMeta frag_meta;
  frag_meta.block_id_ = 1;
  frag_meta.file_id_ = 1;
  frag_meta.offset_ = 0;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 2;
  frag_meta.file_id_ = 2;
  frag_meta.offset_ = 15;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 3;
  frag_meta.file_id_ = 3;
  frag_meta.offset_ = 25;
  frag_meta.size_= 5;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 4;
  frag_meta.file_id_ = 4;
  frag_meta.offset_ = 30;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 5;
  frag_meta.file_id_ = 5;
  frag_meta.offset_ = 40;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 6;
  frag_meta.file_id_ = 6;
  frag_meta.offset_ = 60;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);


  TBSYS_LOG(INFO, "write /test1/test3");
  sprintf(new_dir_path, "/test1/test3");
  ret = service.write(app_id, uid, new_dir_path, fra_info);
  EXPECT_NE(TFS_SUCCESS, ret);
  TBSYS_LOG(INFO, "write /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.write(app_id, uid, new_dir_path, fra_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  fra_info.v_frag_meta_.clear();
  frag_meta.block_id_ = 7;
  frag_meta.file_id_ = 7;
  frag_meta.offset_ = 70;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  TBSYS_LOG(INFO, "write /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.write(app_id, uid, new_dir_path, fra_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  fra_info.v_frag_meta_.clear();
  frag_meta.block_id_ = 8;
  frag_meta.file_id_ = 8;
  frag_meta.offset_ = -1;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  TBSYS_LOG(INFO, "write /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.write(app_id, uid, new_dir_path, fra_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  fra_info.v_frag_meta_.clear();
  frag_meta.block_id_ = 8;
  frag_meta.file_id_ = 8;
  frag_meta.offset_ = 101;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  TBSYS_LOG(INFO, "write /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.write(app_id, uid, new_dir_path, fra_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //now we have  /test1/test3/ /test2
} 
TEST_F(ServiceTest, read_file)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;

  FragInfo frag_info_out;
  bool still_have = true;
  TBSYS_LOG(INFO, "read /test1/test3");
  sprintf(new_dir_path, "/test1/test3");
  ret = service.read(app_id, uid, new_dir_path, 10, 50, frag_info_out, still_have);
  EXPECT_NE(TFS_SUCCESS, ret);
  for (int i = 0; i < frag_info_out.v_frag_meta_.size(); i++)
  {
    TBSYS_LOG(INFO, "block_id %d file_id %ld off_set %ld size %d", 
        frag_info_out.v_frag_meta_[i].block_id_,
        frag_info_out.v_frag_meta_[i].file_id_,
        frag_info_out.v_frag_meta_[i].offset_,
        frag_info_out.v_frag_meta_[i].size_);
  }
  TBSYS_LOG(INFO, "read /test2, offset 10 size 50");
  sprintf(new_dir_path, "/test2");
  ret = service.read(app_id, uid, new_dir_path, 10, 50, frag_info_out, still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  //should get offset 15, 25, 30, 40, 60
  FragInfo fra_info;
  FragMeta frag_meta;
  frag_meta.block_id_ = 2;
  frag_meta.file_id_ = 2;
  frag_meta.offset_ = 15;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 3;
  frag_meta.file_id_ = 3;
  frag_meta.offset_ = 25;
  frag_meta.size_= 5;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 4;
  frag_meta.file_id_ = 4;
  frag_meta.offset_ = 30;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 5;
  frag_meta.file_id_ = 5;
  frag_meta.offset_ = 40;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  frag_meta.block_id_ = 6;
  frag_meta.file_id_ = 6;
  frag_meta.offset_ = 60;
  frag_meta.size_= 10;
  fra_info.v_frag_meta_.push_back(frag_meta);
  ASSERT_EQ(fra_info.v_frag_meta_.size(), frag_info_out.v_frag_meta_.size());
  for (int i = 0; i < frag_info_out.v_frag_meta_.size(); i++)
  {
    TBSYS_LOG(INFO, "block_id %d file_id %ld off_set %ld size %d", 
        frag_info_out.v_frag_meta_[i].block_id_,
        frag_info_out.v_frag_meta_[i].file_id_,
        frag_info_out.v_frag_meta_[i].offset_,
        frag_info_out.v_frag_meta_[i].size_);
    EXPECT_TRUE(fra_info.v_frag_meta_[i].block_id_ == frag_info_out.v_frag_meta_[i].block_id_);
    EXPECT_TRUE(fra_info.v_frag_meta_[i].offset_ == frag_info_out.v_frag_meta_[i].offset_);
    EXPECT_TRUE(fra_info.v_frag_meta_[i].size_ == frag_info_out.v_frag_meta_[i].size_);
  }
  //now we have  /test1/test3/ /test2
} 

TEST_F(ServiceTest, ls)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;

  FragInfo frag_info_out;
  bool still_have = true;
  int64_t pid = -1;
  TBSYS_LOG(INFO, "ls /");
  sprintf(new_dir_path, "/");
  vector<MetaInfo> v_meta_info;
  ret = service.ls(app_id, uid, pid, new_dir_path, DIRECTORY, v_meta_info, still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  for (int i = 0; i < v_meta_info.size(); i++)
  {
    TBSYS_LOG(INFO, "pid %ld id %ld size %ld ver_no_ %ld name_len %d name %.*s", 
        v_meta_info[i].file_info_.pid_, v_meta_info[i].file_info_.id_, v_meta_info[i].file_info_.size_, 
        v_meta_info[i].file_info_.ver_no_, v_meta_info[i].file_info_.name_.length(),
        v_meta_info[i].file_info_.name_.length()-1,v_meta_info[i].file_info_.name_.c_str() + 1);
  }
  TBSYS_LOG(INFO, "ls /test1");
  sprintf(new_dir_path, "/test1");
  v_meta_info.clear();
  ret = service.ls(app_id, uid, pid, new_dir_path, DIRECTORY, v_meta_info, still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  for (int i = 0; i < v_meta_info.size(); i++)
  {
    TBSYS_LOG(INFO, "pid %ld id %ld size %ld ver_no_ %ld name_len %d name %.*s", 
        v_meta_info[i].file_info_.pid_, v_meta_info[i].file_info_.id_, v_meta_info[i].file_info_.size_, 
        v_meta_info[i].file_info_.ver_no_, v_meta_info[i].file_info_.name_.length(),
        v_meta_info[i].file_info_.name_.length()-1,v_meta_info[i].file_info_.name_.c_str() + 1);
  }
  TBSYS_LOG(INFO, "ls /test2");
  sprintf(new_dir_path, "/test2");
  v_meta_info.clear();
  ret = service.ls(app_id, uid, pid, new_dir_path, NORMAL_FILE, v_meta_info, still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  for (int i = 0; i < v_meta_info.size(); i++)
  {
    TBSYS_LOG(INFO, "pid %ld id %ld size %ld ver_no_ %ld name_len %d name %.*s", 
        v_meta_info[i].file_info_.pid_, v_meta_info[i].file_info_.id_, v_meta_info[i].file_info_.size_, 
        v_meta_info[i].file_info_.ver_no_, v_meta_info[i].file_info_.name_.length(),
        v_meta_info[i].file_info_.name_.length()-1,v_meta_info[i].file_info_.name_.c_str() + 1);
  }
} 
int main(int argc, char* argv[])
{
  
  {
    MemHelper::init(5,5,5);
    TBSYS_CONFIG.load("./test_service.conf");
    service.initialize(100, NULL);
    printf("app_id %lu, uid: %lu\n", app_id, uid);
    //TBSYS_LOGGER.setLogLevel("debug");
    PROFILER_SET_STATUS(0);
    testing::InitGoogleTest(&argc, argv);
  }
  return RUN_ALL_TESTS();
}
