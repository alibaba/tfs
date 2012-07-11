/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_blockfile_manager.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   yangye
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "blockfile_manager.h"
#include "gc.h"
#include <unistd.h>

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;

// delete test files and folders
int clearfile(SuperBlock super_block)
{
  char str[16];
  chdir("./disk");
  for (int32_t i = 1; i <= super_block.main_block_count_; i++)
  {
    sprintf(str, "%d", i);
    unlink(str);
  }
  chdir("./extend");
  for (int32_t i = super_block.main_block_count_ + 1; i <= super_block.main_block_count_
      + super_block.extend_block_count_; i++)
  {
    sprintf(str, "%d", i);
    EXPECT_EQ(access(str, 0), 0);
    unlink(str);
  }
  chdir("../");
  chdir("./index");
  for (int32_t i = 1; i <= super_block.main_block_count_; i++)
  {
    sprintf(str, "%d", i);
    unlink(str);
  }
  chdir("../");
  unlink("fs_super");
  rmdir("index");
  rmdir("extend");
  chdir("../");
  rmdir("disk");
  return 0;
}

int set_fs_param(FileSystemParameter &fs_param)
{
  fs_param.mount_name_.assign("./disk");
  fs_param.max_mount_size_ = 20971520;
  fs_param.base_fs_type_ = 3;
  fs_param.super_block_reserve_offset_ = 0;
  fs_param.avg_segment_size_ = 15 * 1024;
  fs_param.main_block_size_ = 16777216;
  fs_param.extend_block_size_ = 1048576;
  fs_param.block_type_ratio_ = 0.5;
  fs_param.file_system_version_ = 1;
  fs_param.hash_slot_ratio_ = 0.5;
  return 0;
}

// insert 100 logic blocks, and will deleted in clear_blocks
int insert_blocks(BlockFileManager* blockfile_manager, std::list<uint32_t>& id_list, BlockType block_type = C_MAIN_BLOCK)
{
  id_list.clear();
  uint32_t physic_id = 0;
  uint32_t logic_id = 0;
  for (int32_t i = 0; i < 100; i++)
  {
    physic_id = 0;
    logic_id = logic_id + random() % 10 + 1;
    blockfile_manager->new_block(logic_id, physic_id, block_type);
    EXPECT_EQ(blockfile_manager->get_logic_block(logic_id, block_type)->get_physic_block_list()->size(), static_cast<uint32_t>(1));
    id_list.push_back(logic_id);
  }
  return 0;
}

int32_t clear_blocks(BlockFileManager *blockfile_manager, std::list<uint32_t>& id_list, BlockType block_type)
{
  int32_t num = 0;
  for (std::list<uint32_t>::iterator mit = id_list.begin(); mit != id_list.end(); ++mit)
  {
    EXPECT_EQ(blockfile_manager->del_block(*mit, block_type), 0) << block_type;
    num++;
  }
  id_list.clear();
  EXPECT_NE(GCObjectManager::instance().object_list_.size(), 0U);
  while(GCObjectManager::instance().object_list_.size() > 0 ) {
    printf("remain gcobject num: %u\n", static_cast<uint32_t>(GCObjectManager::instance().object_list_.size()));
    sleep(SYSPARAM_DATASERVER.object_dead_max_time_);
  }
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);
  return num;
}

TEST(BlockFileManagerIniTest, InitOP)
{
  TBSYS_LOGGER.setLogLevel("error");
  BlockFileManager * blockfile_manager=BlockFileManager::get_instance();//infact nothing has been done
  FileSystemParameter fs_param;
  set_fs_param(fs_param);

  int32_t ret = blockfile_manager->format_block_file_system(fs_param);
  ASSERT_EQ(ret, 0);
  //init_super_blk_param
  //create_fs
  EXPECT_EQ(access("./disk", 0), 0);
  EXPECT_EQ(access("./disk/index", 0), 0);
  EXPECT_EQ(access("./disk/extend", 0), 0);
  //create_block MAIN
  SuperBlock super_block;
  blockfile_manager->query_super_block(super_block);
  chdir("./disk");
  struct stat buf;
  char str[10];
  FILE* read_blk;
  BlockPrefix block_prefix;
  for (int32_t i = 1; i <= super_block.main_block_count_; i++)
  {
    memset(&buf, 0, sizeof(buf));
    memset(&block_prefix, 0, sizeof(BlockPrefix));
    sprintf(str, "%d", i);
    if(stat(str, &buf)<0)
    {
      ADD_FAILURE();
    }
    //head is not being writen
    EXPECT_EQ(buf.st_size, 16777216);
    read_blk = fopen(str, "r");
    fread(reinterpret_cast<char*>(&block_prefix), sizeof(BlockPrefix), 1, read_blk);
    EXPECT_EQ(block_prefix.logic_blockid_, static_cast<uint32_t>(0));
    EXPECT_EQ(block_prefix.prev_physic_blockid_, static_cast<uint32_t>(0));
    EXPECT_EQ(block_prefix.next_physic_blockid_, static_cast<uint32_t>(0));
    fclose(read_blk);
  }
  chdir("./extend");
  //create_block EXT
  for(int32_t i = super_block.main_block_count_ + 1; i <= super_block.main_block_count_ + super_block.extend_block_count_; i++)
  {
    memset(&buf, 0, sizeof(buf));
    sprintf(str, "%d", i);
    if (stat(str, &buf) < 0)
    {
      ADD_FAILURE();
    }
    EXPECT_EQ(buf.st_size, 1048576);
  }
  chdir("../..");

  blockfile_manager->bootstrap(fs_param);
  //load_super_blk
  blockfile_manager->query_super_block(super_block);
  EXPECT_EQ(clearfile(super_block), 0);
  TBSYS_LOGGER.setLogLevel("debug");
}

// normal op
class BlockFileManagerTest: public ::testing::Test
{
protected:
  static void SetUpTestCase();
  //{
  //  TBSYS_LOGGER.setLogLevel("debug");
  //  timer = new tbutil::Timer();
  //  SYSPARAM_DATASERVER.object_dead_max_time_ = 3;
  //  EXPECT_EQ(GCObjectManager::instance().initialize(timer), 0);
  //}

  static void TearDownTestCase();
  //{
  //  TBSYS_LOGGER.setLogLevel("debug");
  //  if (0 != timer)
  //  {
  //    timer->destroy();
  //  }
  //}

public:
  virtual void SetUp() //init op
  {
    blockfile_manager = BlockFileManager::get_instance();
    set_fs_param(fs_param);
    blockfile_manager->format_block_file_system(fs_param);
    blockfile_manager->bootstrap(fs_param);
  }
  virtual void TearDown()
  {
    blockfile_manager->query_super_block(super_block);
    EXPECT_EQ(clearfile(super_block), 0);
  }
protected:
  BlockFileManager* blockfile_manager;
  FileSystemParameter fs_param;
  SuperBlock super_block;
  static tbutil::Timer* timer;
};

tbutil::Timer* BlockFileManagerTest::timer;

void BlockFileManagerTest::SetUpTestCase()
{
  TBSYS_LOGGER.setLogLevel("debug");
  timer = new tbutil::Timer();
  SYSPARAM_DATASERVER.object_dead_max_time_ = 3;
  EXPECT_EQ(GCObjectManager::instance().initialize(timer), 0);
}

void BlockFileManagerTest::TearDownTestCase()
{
  TBSYS_LOGGER.setLogLevel("debug");
  if (0 != timer)
  {
    timer->destroy();
  }
}



TEST_F(BlockFileManagerTest, NewBlocktest)
{
  uint32_t physic_id = 0;
  uint32_t logic_id = 123;
  blockfile_manager->new_block(logic_id, physic_id, C_MAIN_BLOCK);
  // check index handle info
  char index_name[20];
  char blk_name[20];
  char ext_blk_name[20];
  sprintf(index_name, "./disk/index/%d", physic_id);
  sprintf(blk_name, "./disk/%d", physic_id);
  ASSERT_EQ(access(index_name, 0), 0);
  FILE* fp = fopen(index_name, "r");
  IndexHeader index_header;
  memset(reinterpret_cast<char*>(&index_header), 0, sizeof(IndexHeader));
  fread(reinterpret_cast<char*>(&index_header), sizeof(IndexHeader), 1, fp);
  EXPECT_EQ(index_header.block_info_.block_id_, logic_id);
  EXPECT_EQ(index_header.block_info_.version_, 0);
  EXPECT_EQ(index_header.block_info_.file_count_, 0);
  EXPECT_EQ(index_header.block_info_.size_, 0);
  EXPECT_EQ(index_header.block_info_.del_file_count_, 0);
  EXPECT_EQ(index_header.block_info_.del_size_, 0);
  EXPECT_EQ(index_header.block_info_.seq_no_, static_cast<uint32_t>(1));
  EXPECT_EQ(index_header.flag_, C_DATA_CLEAN);
  fclose(fp);
  // check physic block info
  BlockPrefix block_prefix;
  ASSERT_EQ(access(blk_name, 0), 0);
  fp = fopen(blk_name, "r");
  fread(reinterpret_cast<char*>(&block_prefix), sizeof(BlockPrefix), 1, fp);
  EXPECT_EQ(block_prefix.logic_blockid_, logic_id);
  EXPECT_EQ(block_prefix.prev_physic_blockid_, static_cast<uint32_t>(0));
  EXPECT_EQ(block_prefix.next_physic_blockid_, static_cast<uint32_t>(0));
  fclose(fp);
  // new ext block
  uint32_t ext_id = 0;
  PhysicalBlock* physic_block;
  blockfile_manager->new_ext_block(logic_id, physic_id, ext_id, &physic_block);
  LogicBlock* logic_block = blockfile_manager->get_logic_block(logic_id, C_MAIN_BLOCK);
  logic_block->add_physic_block(physic_block);
  EXPECT_EQ(logic_block->get_physic_block_list()->size(), static_cast<uint32_t>(2));
  ASSERT_EQ(access(blk_name, 0), 0);
  fp = fopen(blk_name, "r");
  fread(reinterpret_cast<char*>(&block_prefix), sizeof(BlockPrefix), 1, fp);
  EXPECT_EQ(block_prefix.logic_blockid_, logic_id);
  EXPECT_EQ(block_prefix.prev_physic_blockid_, static_cast<uint32_t>(0));
  EXPECT_EQ(block_prefix.next_physic_blockid_, ext_id);
  fclose(fp);
  sprintf(ext_blk_name, "./disk/extend/%d", ext_id);
  ASSERT_EQ(access(ext_blk_name, 0), 0);
  fp = fopen(ext_blk_name, "r");
  fread(reinterpret_cast<char*>(&block_prefix), sizeof(BlockPrefix), 1, fp);
  EXPECT_EQ(block_prefix.logic_blockid_, logic_id);
  EXPECT_EQ(block_prefix.prev_physic_blockid_, physic_id);
  EXPECT_EQ(block_prefix.next_physic_blockid_, static_cast<uint32_t>(0));
  fclose(fp);
  ext_id = 0;

  // check gc
  uint32_t old_size = GCObjectManager::instance().object_list_.size();
  blockfile_manager->del_block(logic_id, C_MAIN_BLOCK);
  uint32_t new_size = GCObjectManager::instance().object_list_.size();
  EXPECT_EQ(new_size - old_size, 1U);
  EXPECT_NE(access(index_name, 0), 0);
  sleep(1);
  //sleep(2*SYSPARAM_DATASERVER.object_dead_max_time_);
  //EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);

  uint32_t compact_id = 0;
  blockfile_manager->new_block(logic_id, compact_id, C_COMPACT_BLOCK);

  // check index handle info
  sprintf(index_name, "./disk/index/%d", compact_id);
  sprintf(blk_name, "./disk/%d", compact_id);
  ASSERT_EQ(access(index_name, 0), 0);
  fp = fopen(index_name, "r");
  memset(reinterpret_cast<char*>(&index_header), 0, sizeof(IndexHeader));
  fread(reinterpret_cast<char*>(&index_header), sizeof(IndexHeader), 1, fp);
  EXPECT_EQ(index_header.block_info_.block_id_, logic_id);
  EXPECT_EQ(index_header.block_info_.version_, 0);
  EXPECT_EQ(index_header.block_info_.file_count_, 0);
  EXPECT_EQ(index_header.block_info_.size_, 0);
  EXPECT_EQ(index_header.block_info_.del_file_count_, 0);
  EXPECT_EQ(index_header.block_info_.del_size_, 0);
  EXPECT_EQ(index_header.block_info_.seq_no_, static_cast<uint32_t>(1));
  EXPECT_EQ(index_header.flag_, C_DATA_COMPACT);
  fclose(fp);

  // check physic block info
  memset(reinterpret_cast<char*>(&block_prefix), 0, sizeof(BlockPrefix));
  ASSERT_EQ(access(blk_name, 0), 0);
  fp = fopen(blk_name, "r");
  fread(reinterpret_cast<char*>(&block_prefix), sizeof(BlockPrefix), 1, fp);
  EXPECT_EQ(block_prefix.logic_blockid_, logic_id);
  EXPECT_EQ(block_prefix.prev_physic_blockid_, static_cast<uint32_t>(0));
  EXPECT_EQ(block_prefix.next_physic_blockid_, static_cast<uint32_t>(0));
  fclose(fp);

  // check gc
  old_size = GCObjectManager::instance().object_list_.size();
  blockfile_manager->del_block(logic_id, C_COMPACT_BLOCK);
  new_size = GCObjectManager::instance().object_list_.size();
  EXPECT_EQ(new_size - old_size, 1U);
  EXPECT_NE(access(index_name, 0), 0);
  sleep(2*SYSPARAM_DATASERVER.object_dead_max_time_);
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);
}

TEST_F(BlockFileManagerTest, DelBlocktest)
{
  // create one
  uint32_t physic_id = 0;
  uint32_t logic_id = 223;
  blockfile_manager->new_block(logic_id, physic_id, C_MAIN_BLOCK);
  // check the index_header
  char index_name[20];

  sprintf(index_name, "./disk/index/%d", physic_id);

  FILE* fp = fopen(index_name, "r");
  IndexHeader index_header;
  memset(reinterpret_cast<char*>(&index_header), 0, sizeof(IndexHeader));
  fread(reinterpret_cast<char*>(&index_header), sizeof(IndexHeader), 1, fp);
  EXPECT_EQ(index_header.block_info_.block_id_, logic_id);
  EXPECT_EQ(index_header.block_info_.version_, 0);
  EXPECT_EQ(index_header.block_info_.file_count_, 0);
  EXPECT_EQ(index_header.block_info_.size_, 0);
  EXPECT_EQ(index_header.block_info_.del_file_count_, 0);
  EXPECT_EQ(index_header.block_info_.del_size_, 0);
  EXPECT_EQ(index_header.block_info_.seq_no_, static_cast<uint32_t>(1));
  EXPECT_EQ(index_header.flag_, C_DATA_CLEAN);
  fclose(fp);
  // delete it
  uint32_t old_size = GCObjectManager::instance().object_list_.size();
  blockfile_manager->del_block(logic_id, C_MAIN_BLOCK);
  uint32_t new_size = GCObjectManager::instance().object_list_.size();
  EXPECT_EQ(new_size - old_size, 1U);
  EXPECT_NE(access(index_name, 0), 0);
  sleep(2*SYSPARAM_DATASERVER.object_dead_max_time_);
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);

  uint32_t compact_id = 0;
  blockfile_manager->new_block(logic_id, compact_id, C_COMPACT_BLOCK);
  // check the index_header
  sprintf(index_name, "./disk/index/%d", compact_id);
  fp=fopen(index_name, "r");
  memset(reinterpret_cast<char*>(&index_header), 0, sizeof(IndexHeader));
  fread(reinterpret_cast<char*>(&index_header), sizeof(IndexHeader), 1, fp);
  EXPECT_EQ(index_header.block_info_.block_id_, logic_id);
  EXPECT_EQ(index_header.block_info_.version_, 0);
  EXPECT_EQ(index_header.block_info_.file_count_, 0);
  EXPECT_EQ(index_header.block_info_.size_, 0);
  EXPECT_EQ(index_header.block_info_.del_file_count_, 0);
  EXPECT_EQ(index_header.block_info_.del_size_, 0);
  EXPECT_EQ(index_header.block_info_.seq_no_, static_cast<uint32_t>(1));
  EXPECT_EQ(index_header.flag_, C_DATA_COMPACT);
  fclose(fp);
  //delete it
  old_size = GCObjectManager::instance().object_list_.size();
  blockfile_manager->del_block(logic_id, C_COMPACT_BLOCK); //type :3
  new_size = GCObjectManager::instance().object_list_.size();
  EXPECT_EQ(new_size - old_size, 1U);
  // check gc
  EXPECT_NE(access(index_name, 0), 0);
  sleep(2*SYSPARAM_DATASERVER.object_dead_max_time_);
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);
}

TEST_F(BlockFileManagerTest, GetLogicBlocktest)
{
  std::list<uint32_t> id_list;
  insert_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
  LogicBlock* logic_block;
  for (std::list<uint32_t>::iterator mit = id_list.begin(); mit != id_list.end(); ++mit)
  {
    logic_block = blockfile_manager->get_logic_block(*mit, C_MAIN_BLOCK);
    EXPECT_EQ(logic_block->get_logic_block_id(), *mit);
  }
  int32_t count = 0;
  std::list<uint32_t> cmp_list;
  insert_blocks(blockfile_manager, cmp_list, C_COMPACT_BLOCK);
  for (std::list<uint32_t>::iterator mit = cmp_list.begin(); mit != cmp_list.end(); ++mit)
  {
    logic_block = blockfile_manager->get_logic_block(*mit, C_COMPACT_BLOCK);
    EXPECT_EQ(logic_block->get_logic_block_id(), *mit);
  }
  //get_all_logic_block
  std::list<LogicBlock*> logic_block_list;
  std::list<uint32_t>::iterator block_id;
  blockfile_manager->get_all_logic_block(logic_block_list, C_MAIN_BLOCK);
  EXPECT_EQ(logic_block_list.size(), static_cast<uint32_t>(100));
  int32_t time = 0;
  for (std::list<LogicBlock*>::iterator mit = logic_block_list.begin(); mit != logic_block_list.end(); ++mit)
  {
    block_id = find(id_list.begin(), id_list.end(), (*mit)->get_logic_block_id());
    if(block_id != id_list.end())
    {
      time++;
    }
  }
  EXPECT_EQ(time , 100);
  blockfile_manager->get_all_logic_block(logic_block_list, C_COMPACT_BLOCK);
  EXPECT_EQ(logic_block_list.size(), static_cast<uint32_t>(100));
  for (std::list<LogicBlock*>::iterator mit = logic_block_list.begin(); mit != logic_block_list.end(); ++mit)
  {
    block_id = find(cmp_list.begin(), cmp_list.end(), (*mit)->get_logic_block_id());
    if(block_id != cmp_list.end())
    {
      time--;
    }
  }
  EXPECT_EQ(time , 0);
  // get_logic_block_ids
  VUINT logic_block_ids;
  blockfile_manager->get_logic_block_ids(logic_block_ids);
  for (std::vector<uint32_t>::iterator lbid = logic_block_ids.begin(); lbid != logic_block_ids.end(); lbid++)
  {
    block_id = find(id_list.begin(), id_list.end(), *lbid);
    if(block_id != id_list.end())
    {
      time++;
    }
  }
  EXPECT_EQ(time, 100);
  blockfile_manager->get_logic_block_ids(logic_block_ids, C_COMPACT_BLOCK);
  for (std::vector<uint32_t>::iterator lbid = logic_block_ids.begin(); lbid != logic_block_ids.end(); lbid++)
  {
    block_id = find(cmp_list.begin(), cmp_list.end(), *lbid);
    if(block_id != cmp_list.end())
    {
      time--;
    }
  }
  EXPECT_EQ(time, 0);
  count += clear_blocks(blockfile_manager, cmp_list, C_COMPACT_BLOCK);
  count += clear_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
  EXPECT_EQ(count, 200);
}
class compare
{
public:
  compare(int32_t id) :
    physic_id(id)
  {
  };
  uint32_t physic_id;
  bool operator()(const PhysicalBlock* physicalblock)
  {
    return physicalblock->get_physic_block_id() == physic_id;
  }
};

TEST_F(BlockFileManagerTest, GetPhysicBlocktest)
{
  LogicBlock* logic_block;
  std::list<uint32_t> id_list;
  insert_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
  std::list<uint32_t> cmp_list;
  insert_blocks(blockfile_manager, cmp_list, C_COMPACT_BLOCK);
  // get physic block
  list<PhysicalBlock*> physic_block_list;
  list<PhysicalBlock*>* temp_block_list;
  physic_block_list.clear();
  blockfile_manager->get_all_physic_block(physic_block_list);
  std::list<PhysicalBlock*>::iterator result;
  uint32_t size = physic_block_list.size();
  for (std::list<uint32_t>::iterator mit = id_list.begin(); mit != id_list.end(); ++mit)
  {
    logic_block = blockfile_manager->get_logic_block(*mit, C_MAIN_BLOCK);
    temp_block_list = logic_block->get_physic_block_list();
    for (std::list<PhysicalBlock*>::iterator block = temp_block_list->begin(); block != temp_block_list->end(); ++block)
    {
      physic_block_list.remove_if(compare((*block)->get_physic_block_id()));
    }
    size -= temp_block_list->size();
  }
  for (std::list<uint32_t>::iterator mit = cmp_list.begin(); mit != cmp_list.end(); ++mit)
  {
    logic_block = blockfile_manager->get_logic_block(*mit, C_COMPACT_BLOCK);
    temp_block_list = logic_block->get_physic_block_list();
    for (std::list<PhysicalBlock*>::iterator block = temp_block_list->begin(); block != temp_block_list->end(); ++block)
    {
      physic_block_list.remove_if(compare((*block)->get_physic_block_id()));
    }
    size -= temp_block_list->size();
  }
  EXPECT_EQ(size, physic_block_list.size());
  EXPECT_TRUE(physic_block_list.empty());
  EXPECT_EQ(static_cast<uint32_t>(0), physic_block_list.size());
  clear_blocks(blockfile_manager, cmp_list, C_COMPACT_BLOCK);
  clear_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
}

TEST_F(BlockFileManagerTest, QueryInfotest)
{
  SuperBlock super_block_info;
  memset(&super_block_info, 0, sizeof(SuperBlock));
  //query_super_block
  blockfile_manager->query_super_block(super_block_info);
  EXPECT_EQ(strcmp(super_block_info.mount_tag_, "TAOBAO"), 0);
  EXPECT_EQ(strcmp(super_block_info.mount_point_, "./disk"), 0);
  int64_t mountsize = 20971520;
  mountsize *= 1024;
  EXPECT_EQ(super_block_info.mount_point_use_space_, mountsize);
  EXPECT_EQ(super_block_info.base_fs_type_, 3);
  EXPECT_EQ(super_block_info.superblock_reserve_offset_, 0);
  EXPECT_EQ(super_block_info.bitmap_start_offset_, static_cast<int32_t>(2 * sizeof(SuperBlock) + sizeof(int32_t)));
  EXPECT_EQ(super_block_info.avg_segment_size_, 15360); //size is 0, error
  EXPECT_EQ(super_block_info.block_type_ratio_, 0.5);
  EXPECT_EQ(super_block_info.main_block_count_, 1131);
  EXPECT_EQ(super_block_info.main_block_size_, 16777216);
  EXPECT_EQ(super_block_info.extend_block_count_, 2263);
  EXPECT_EQ(super_block_info.extend_block_size_, 1048576);
  EXPECT_EQ(super_block_info.used_block_count_, 0);
  EXPECT_EQ(super_block_info.used_extend_block_count_, 0);
  EXPECT_EQ(super_block_info.hash_slot_ratio_, 0.5);
  EXPECT_EQ(super_block_info.hash_slot_size_, 614);
  EXPECT_EQ(super_block_info.version_, 1);
  // query_approx_block_count
  int32_t bcount = 0;
  blockfile_manager->query_approx_block_count(bcount);
  EXPECT_EQ(bcount, 0);
  // query_space
  int64_t used_bytes = 0;
  int64_t total_bytes = 0;
  blockfile_manager->query_space(used_bytes, total_bytes);
  EXPECT_EQ(used_bytes, 0);
  EXPECT_EQ(total_bytes, 21347958784LL);

  std::list<uint32_t> id_list;
  insert_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);

  blockfile_manager->query_approx_block_count(bcount);
  EXPECT_EQ(bcount, 100);
  blockfile_manager->query_space(used_bytes, total_bytes);
  int64_t used_size = 100;
  used_size *= 16777216;
  // ratio=0.5, so 2*main_block_count*ext_block_size
  used_size += 104857600L;
  used_size += 104857600L;
  EXPECT_EQ(used_bytes, used_size);
  EXPECT_EQ(total_bytes, 21347958784LL);
  clear_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
}

TEST_F(BlockFileManagerTest, LoadSuperBlocktest)
{
  FileSystemParameter fs_param;
  fs_param.mount_name_ = "./disk";
  fs_param.super_block_reserve_offset_ = 0;
  blockfile_manager->load_super_blk(fs_param);
  // load_super_blk
  SuperBlock super_block_info;
  memset(&super_block_info, 0, sizeof(SuperBlock));
  // query_super_block
  blockfile_manager->query_super_block(super_block_info);
  EXPECT_EQ(strcmp(super_block_info.mount_tag_, "TAOBAO"), 0);
  EXPECT_EQ(strcmp(super_block_info.mount_point_, "./disk"), 0);
  int64_t mountsize = 20971520;
  mountsize *= 1024;
  EXPECT_EQ(super_block_info.mount_point_use_space_, mountsize);
  EXPECT_EQ(super_block_info.base_fs_type_, 3);
  EXPECT_EQ(super_block_info.superblock_reserve_offset_, 0);
  EXPECT_EQ(super_block_info.bitmap_start_offset_, static_cast<int32_t>(2 * sizeof(SuperBlock) + sizeof(int32_t)));
  EXPECT_EQ(super_block_info.avg_segment_size_, 15*1024); //error again
  EXPECT_EQ(super_block_info.block_type_ratio_, 0.5);
  EXPECT_EQ(super_block_info.main_block_count_, 1131);
  EXPECT_EQ(super_block_info.main_block_size_, 16777216);
  EXPECT_EQ(super_block_info.extend_block_count_, 2263);
  EXPECT_EQ(super_block_info.extend_block_size_, 1048576);
  EXPECT_EQ(super_block_info.used_block_count_, 0);
  EXPECT_EQ(super_block_info.used_extend_block_count_, 0);
  EXPECT_EQ(super_block_info.hash_slot_ratio_, 0.5);
  EXPECT_EQ(super_block_info.hash_slot_size_, 614);
  EXPECT_EQ(super_block_info.version_, 1);
}

TEST_F(BlockFileManagerTest, CompactBlocktest)
{
  // switch_compact_blk   OK
  uint32_t logic_id = 456;
  uint32_t physic_id = 0;
  blockfile_manager->new_block(logic_id, physic_id, C_MAIN_BLOCK);
  uint32_t cmp_id = 0;
  blockfile_manager->new_block(logic_id, cmp_id, C_COMPACT_BLOCK);

  blockfile_manager->switch_compact_blk(logic_id);
  EXPECT_EQ(blockfile_manager->get_logic_block(logic_id, C_MAIN_BLOCK)->get_physic_block_list()->front()->get_physic_block_id(), cmp_id);
  EXPECT_EQ(blockfile_manager->get_logic_block(logic_id, C_COMPACT_BLOCK)->get_physic_block_list()->front()->get_physic_block_id(), physic_id);

  std::set<uint32_t> erase_blocks;
  blockfile_manager->expire_compact_blk(time(NULL), erase_blocks); //need lots of blocks
  // delete block
  blockfile_manager->del_block(logic_id, C_MAIN_BLOCK);
  blockfile_manager->del_block(logic_id, C_COMPACT_BLOCK);
  // check gc
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 2U);
  sleep(3*SYSPARAM_DATASERVER.object_dead_max_time_);
  EXPECT_EQ(GCObjectManager::instance().object_list_.size(), 0U);
}

TEST_F(BlockFileManagerTest, BitMaptest)
{
  std::list<uint32_t> id_list;
  insert_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
  // query_bit_map
  char* bit_map;
  int32_t bit_map_len = 0;
  int32_t set_count = 0;
  uint32_t count = 0;
  SuperBlock super_block_info;
  memset(&super_block_info, 0, sizeof(SuperBlock));
  blockfile_manager->query_super_block(super_block_info);
  count += super_block_info.main_block_count_;
  count += super_block_info.extend_block_count_;

  blockfile_manager->query_bit_map(& bit_map, bit_map_len, set_count, C_ALLOCATE_BLOCK);
  EXPECT_EQ(bit_map_len, static_cast<int32_t>((count+7)/8));
  EXPECT_EQ(set_count, 100);
  BitMap bitmap(count);
  bitmap.copy(bit_map_len, bit_map);

  list<uint32_t>::iterator iditr;
  list<PhysicalBlock*> physic_block_list;
  physic_block_list.clear();
  blockfile_manager->get_all_physic_block(physic_block_list);
  list<PhysicalBlock*>::iterator pblock=physic_block_list.begin();
  for (; pblock!=physic_block_list.end(); pblock++)
  {
    iditr = find(id_list.begin(), id_list.end(), (*pblock)->get_physic_block_id());
    if (iditr != id_list.end())
    {
      EXPECT_TRUE(bitmap.test((*pblock)->get_physic_block_id()));
    }
  }

  char* error_map;
  blockfile_manager->query_bit_map(& error_map, bit_map_len, set_count, C_ERROR_BLOCK);
  EXPECT_EQ(bit_map_len, static_cast<int32_t>((count+7)/8));
  EXPECT_EQ(set_count, 0);

  std::set<uint32_t> error_blocks;
  BitMap errormap(count);
  for (uint32_t i = 1; i < 40; i++)
  {
    error_blocks.insert(i);
  }

  blockfile_manager->set_error_bitmap(error_blocks);
  blockfile_manager->query_bit_map(& error_map, bit_map_len, set_count, C_ERROR_BLOCK);
  errormap.copy(bit_map_len, error_map);
  for (uint32_t i = 1; i < 40; i++)
  {
    EXPECT_TRUE(errormap.test(i));
  }

  blockfile_manager->reset_error_bitmap(error_blocks);
  blockfile_manager->query_bit_map(& error_map, bit_map_len, set_count, C_ERROR_BLOCK);
  EXPECT_EQ(set_count, 0);
  BitMap resetmap(count);
  resetmap.copy(bit_map_len, error_map);
  EXPECT_EQ(resetmap.get_set_count(), static_cast<uint32_t>(0));
  for (uint32_t i = 1; i < 40; i++)
  {
    EXPECT_FALSE(resetmap.test(i));
  }

  clear_blocks(blockfile_manager, id_list, C_MAIN_BLOCK);
}

int main(int argc, char* argv[])
{
  testing::GTEST_FLAG(catch_exceptions) = 1;
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
