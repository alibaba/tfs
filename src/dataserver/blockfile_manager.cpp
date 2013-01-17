/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blockfile_manager.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#include "blockfile_manager.h"
#include "blockfile_format.h"
#include "gc.h"
#include "common/directory_op.h"
#include <string.h>
#include <Memory.hpp>
#include <strings.h>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace std;

    BlockFileManager::~BlockFileManager()
    {
      tbsys::gDelete(normal_bit_map_);
      tbsys::gDelete(error_bit_map_);
      tbsys::gDelete(super_block_impl_);

      destruct_logic_blocks(C_COMPACT_BLOCK);
      destruct_logic_blocks(C_MAIN_BLOCK);
      destruct_physic_blocks();
    }

    void BlockFileManager::destruct_logic_blocks(const BlockType block_type)
    {
      LogicBlockMap* selected_logic_blocks = NULL;
      if (C_COMPACT_BLOCK == block_type)
      {
        selected_logic_blocks = &compact_logic_blocks_;
      }
      else // base on current types, main Block
      {
        selected_logic_blocks = &logic_blocks_;
      }

      for (LogicBlockMapIter mit = selected_logic_blocks->begin(); mit != selected_logic_blocks->end(); ++mit)
      {
        tbsys::gDelete(mit->second);
      }
    }

    void BlockFileManager::destruct_physic_blocks()
    {
      for (PhysicalBlockMapIter mit = physical_blocks_.begin(); mit != physical_blocks_.end(); ++mit)
      {
        tbsys::gDelete(mit->second);
      }
    }

    int BlockFileManager::format_block_file_system(const FileSystemParameter& fs_param, const bool speedup)
    {
      // 1. initialize super block parameter
      int ret = init_super_blk_param(fs_param);
      if (TFS_SUCCESS != ret)
        return ret;

      // 2. create mount directory
      ret = create_fs_dir();
      if (TFS_SUCCESS != ret)
        return ret;

      // 3. create super block file
      ret = create_fs_super_blk();
      if (TFS_SUCCESS != ret)
        return ret;

      // 4. pre-allocate create main block
      ret = create_block(C_MAIN_BLOCK);
      if (TFS_SUCCESS != ret)
        return ret;

      // 5. pre-allocate create extend block
      ret = create_block(C_EXT_BLOCK);
      if (TFS_SUCCESS != ret)
        return ret;

      // 6. create block_prefix file
      if (speedup)
      {
        ret = create_block_prefix();
        if (TFS_SUCCESS != ret)
          return ret;
      }

      return TFS_SUCCESS;
    }

    int BlockFileManager::clear_block_file_system(const FileSystemParameter& fs_param)
    {
      bool ret = DirectoryOp::delete_directory_recursively(fs_param.mount_name_.c_str());
      TBSYS_LOG(INFO, "clear block file system end. mount_point: %s, ret: %d", fs_param.mount_name_.c_str(), ret);
      return ret ? TFS_SUCCESS : TFS_ERROR;
    }

    int BlockFileManager::index_filter(const struct dirent *entry)
    {
      int exist = 0;
      if (index(entry->d_name, '.'))
      {
        exist = 1;
      }
      return exist;
    }

    void BlockFileManager::clear_block_tmp_index(const char* mount_path)
    {
      TBSYS_LOG(INFO, "clearing temp index file.");
      int num = 0;
      struct dirent** namelist = NULL;
      string index_dir(mount_path);
      index_dir += INDEX_DIR_PREFIX;
      num = scandir(index_dir.c_str(), &namelist, index_filter, NULL);
      for (int i = 0; i < num; i++)
      {
        string filename = index_dir + namelist[i]->d_name;
        unlink(filename.c_str());  // ignore return value
      }

      for (int i = 0; i < num; i++)
      {
        tbsys::gFree(namelist[i]);
      }

      tbsys::gFree(namelist);
    }

    int BlockFileManager::bootstrap(const FileSystemParameter& fs_param)
    {
      // 1. load super block
      int ret = load_super_blk(fs_param);
      if (TFS_SUCCESS != ret)
        return ret;

      // 2. load block file, create logic block map associate stuff
      std::string mount_path(super_block_.mount_point_);
      ret = PhysicalBlock::init_prefix_op(mount_path);
      if (TFS_SUCCESS != ret)
        return ret;
      clear_block_tmp_index(mount_path.c_str());
      return load_block_file();
    }

    int BlockFileManager::new_block(const uint32_t logic_block_id, uint32_t& physical_block_id,
        const BlockType block_type)
    {
      // 1. write block
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      LogicBlockMap* selected_logic_blocks = NULL;

      // 2. get right logic block id handle
      if (C_COMPACT_BLOCK == block_type)
      {
        selected_logic_blocks = &compact_logic_blocks_;
      }
      else //main block, half block
      {
        selected_logic_blocks = &logic_blocks_;
      }

      LogicBlockMapIter lmit = selected_logic_blocks->find(logic_block_id);
      if (lmit != selected_logic_blocks->end())
      {
        TBSYS_LOG(ERROR, "logic block has already exist! logic blockid: %u", logic_block_id);
        return EXIT_BLOCK_EXIST_ERROR;
      }

      // 3. find avial physical block
      int ret = find_avail_block(physical_block_id, C_MAIN_BLOCK);
      if (TFS_SUCCESS != ret)
        return ret;

      PhysicalBlockMapIter pmit = physical_blocks_.find(physical_block_id);
      // oops, same physical block id found
      if (pmit != physical_blocks_.end())
      {
        TBSYS_LOG(ERROR, "bitmap and physical blocks conflict. fatal error! physical blockid: %u", physical_block_id);
        assert(false);
      }

      // 4. create physical block
      PhysicalBlock* t_physical_block = new PhysicalBlock(physical_block_id, super_block_.mount_point_,
          super_block_.main_block_size_, C_MAIN_BLOCK);
      t_physical_block->set_block_prefix(logic_block_id, 0, 0);
      ret = t_physical_block->dump_block_prefix();
      if (ret)
      {
        TBSYS_LOG(ERROR, "dump physical block fail. fatal error! pos: %u, file: %s, ret: %d", physical_block_id,
            super_block_.mount_point_, ret);
        tbsys::gDelete(t_physical_block);
        return ret;
      }

      // 5. create logic block
      LogicBlock* t_logic_block = new LogicBlock(logic_block_id, physical_block_id, super_block_.mount_point_);
      t_logic_block->add_physic_block(t_physical_block);

      TBSYS_LOG(INFO,
          "new block. logic blockid: %u, physical blockid: %u, physci prev blockid: %u, physci next blockid: %u",
          logic_block_id, physical_block_id, 0, 0);

      bool block_count_modify_flag = false;
      do
      {
        // 6. set normal bitmap: have not serialize to disk
        normal_bit_map_->set(physical_block_id);

        // 7. write normal bitmap
        ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_);
        if (TFS_SUCCESS != ret)
          break;

        // 8. update and write super block info
        ++super_block_.used_block_count_;
        block_count_modify_flag = true;
        ret = super_block_impl_->write_super_blk(super_block_);
        if (TFS_SUCCESS != ret)
          break;

        // 9. sync to disk
        ret = super_block_impl_->flush_file();
        if (TFS_SUCCESS != ret)
          break;

        // 10. init logic block (create index handle file, etc stuff)
        ret = t_logic_block->init_block_file(super_block_.hash_slot_size_, super_block_.mmap_option_, block_type);
        if (TFS_SUCCESS != ret)
          break;

        // 11. insert to associate map
        physical_blocks_.insert(PhysicalBlockMap::value_type(physical_block_id, t_physical_block));
        selected_logic_blocks->insert(LogicBlockMapIter::value_type(logic_block_id, t_logic_block));

        TBSYS_LOG(INFO, "new block success! logic blockid: %u, physical blockid: %u.", logic_block_id,
                  physical_block_id);
      }
      while (0);

      // oops,new block fail,clean.
      // at this point, step 11 has not arrived. must delete here
      if (ret)
      {
        TBSYS_LOG(ERROR, "new block fail. logic blockid: %u. ret: %d", logic_block_id, ret);
        rollback_superblock(physical_block_id, block_count_modify_flag);
        t_logic_block->delete_block_file();

        tbsys::gDelete(t_logic_block);
        tbsys::gDelete(t_physical_block);
      }
      return ret;
    }

    int BlockFileManager::del_block(const uint32_t logic_block_id, const BlockType block_type)
    {
      TBSYS_LOG(INFO, "delete block! logic blockid: %u. blocktype: %d", logic_block_id, block_type);
      // 1. ...
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      LogicBlock* delete_block = NULL;
      BlockType tmp_block_type = block_type;
      // 2. choose right type block to delete
      delete_block = choose_del_block(logic_block_id, tmp_block_type);
      if (NULL == delete_block)
      {
        TBSYS_LOG(ERROR, "can not find logic blockid: %u. blocktype: %d when delete block", logic_block_id,
                  tmp_block_type);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      // 3. logic block layer lock
      delete_block->wlock(); //lock this block
      // 4. get physical block list
      list<PhysicalBlock*>* physic_block_list = delete_block->get_physic_block_list();
      int size = physic_block_list->size();
      if (0 == size)
      {
        delete_block->unlock();
        return EXIT_PHYSICALBLOCK_NUM_ERROR;
      }

      list<PhysicalBlock*>::iterator lit = physic_block_list->begin();
      list<PhysicalBlock*> tmp_physic_block;
      tmp_physic_block.clear();

      // 5. erase(but not clean) physic block from physic map
      for (; lit != physic_block_list->end(); ++lit)
      {
        uint32_t physic_id = (*lit)->get_physic_block_id();
        TBSYS_LOG(INFO, "blockid: %u, del physical block! physic blockid: %u.", logic_block_id, physic_id);

        PhysicalBlockMapIter mpit = physical_blocks_.find(physic_id);
        if (mpit == physical_blocks_.end())
        {
          TBSYS_LOG(ERROR, "can not find physical block! physic blockid: %u.", physic_id);
          assert(false);
        }
        else
        {
          if (mpit->second)
          {
            tmp_physic_block.push_back(mpit->second);
          }
          physical_blocks_.erase(mpit);
        }
        // normal bitmap clear reset
        normal_bit_map_->reset(physic_id);
      }

      // 7. delete logic block from logic block map
      erase_logic_block(logic_block_id, tmp_block_type);

      int ret = TFS_SUCCESS;
      do
      {
        // 8. write bitmap
        ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_);
        if (TFS_SUCCESS != ret)
          break;
        // 9. update and write superblock info
        super_block_.used_block_count_ -= 1;
        super_block_.used_extend_block_count_ -= (size - 1);
        ret = super_block_impl_->write_super_blk(super_block_);
        if (TFS_SUCCESS != ret)
          break;

        // 10. flush suplerblock
        ret = super_block_impl_->flush_file();
      }
      while (0);

      TBSYS_LOG(INFO, "logicblock delete %s! logic blockid: %u. physical block size: %d, blocktype: %d, ret: %d",
                ret ? "fail" : "success", logic_block_id, size, tmp_block_type, ret);

      // 11. unlock
      if (delete_block)
      {
        delete_block->unlock();
      }
      // 12. if gc init, delay delete pointer, unlock & add logic block into gcobject manager
      if (GCObjectManager::instance().is_init())
      {
        if (delete_block)
        {
          delete_block->set_dead_time();
          GCObjectManager::instance().add(delete_block);
          delete_block->rename_index_file();
        }
      }
      // else, delete pointer
      else
      {
        tbsys::gDelete(delete_block);

        for (list<PhysicalBlock*>::iterator lit = tmp_physic_block.begin(); lit != tmp_physic_block.end(); ++lit)
        {
          tbsys::gDelete(*lit);
        }
      }

      return ret;
    }

    int BlockFileManager::new_ext_block(const uint32_t logic_block_id, const uint32_t physical_block_id,
                                        uint32_t& ext_physical_block_id, PhysicalBlock **physic_block)
    {
      // 1. ...
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      BlockType block_type;
      if (0 != physical_block_id)
      {
        block_type = C_EXT_BLOCK;
      }
      else
      {
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      // 2. ...
      LogicBlockMapIter mit = logic_blocks_.find(logic_block_id);
      if (mit == logic_blocks_.end())
      {
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      // 3. get available extend physic block
      ret = find_avail_block(ext_physical_block_id, block_type);
      if (TFS_SUCCESS != ret)
        return ret;

      PhysicalBlockMapIter pmit = physical_blocks_.find(ext_physical_block_id);
      if (pmit != physical_blocks_.end())
      {
        TBSYS_LOG(ERROR, "physical block conflict. fatal error! ext physical blockid: %u", ext_physical_block_id);
        assert(false);
      }

      // 4. make sure physical_block_id exist
      pmit = physical_blocks_.find(physical_block_id);
      if (pmit == physical_blocks_.end())
      {
        TBSYS_LOG(ERROR, "can not find physical blockid: %u", physical_block_id);
        assert(false);
      }

      if (NULL == pmit->second)
      {
        TBSYS_LOG(ERROR, "physical blockid: %u point null", physical_block_id);
        assert(false);
      }

      // 5. create new physic block
      PhysicalBlock* tmp_physical_block = new PhysicalBlock(ext_physical_block_id, super_block_.mount_point_,
                                                            super_block_.extend_block_size_, C_EXT_BLOCK);

      TBSYS_LOG(INFO, "new ext block. logic blockid: %u, prev physical blockid: %u, now physical blockid: %u",
                logic_block_id, physical_block_id, ext_physical_block_id);

      bool block_count_modify_flag = false;
      do
      {
        // 6. bitmap set
        normal_bit_map_->set(ext_physical_block_id);

        // 7. write bitmap
        ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_);
        if (TFS_SUCCESS != ret)
          break;

        // 8. update & write superblock info
        ++super_block_.used_extend_block_count_;
        block_count_modify_flag = true;
        ret = super_block_impl_->write_super_blk(super_block_);
        if (TFS_SUCCESS != ret)
          break;

        // 9. sync to disk
        ret = super_block_impl_->flush_file();
        if (TFS_SUCCESS != ret)
          break;

        // 10. write physical block info to disk
        tmp_physical_block->set_block_prefix(logic_block_id, physical_block_id, 0);
        ret = tmp_physical_block->dump_block_prefix();
        if (TFS_SUCCESS != ret)
          break;

        // 11. add to extend block list
        // get previous physcial block and record the next block
        PhysicalBlock* prev_physic_block = pmit->second;
        prev_physic_block->set_next_block(ext_physical_block_id);
        // write prev block info to disk
        ret = prev_physic_block->dump_block_prefix();
        if (TFS_SUCCESS != ret)
          break;

        // 12. insert to physic block map
        physical_blocks_.insert(PhysicalBlockMap::value_type(ext_physical_block_id, tmp_physical_block));
        (*physic_block) = tmp_physical_block;
      }
      while (0);

      if (TFS_SUCCESS != ret)   // not arrive step 12
      {
        TBSYS_LOG(ERROR, "new ext block error! logic blockid: %u. ret: %d", logic_block_id, ret);
        rollback_superblock(ext_physical_block_id, block_count_modify_flag, C_EXT_BLOCK);
        tbsys::gDelete(tmp_physical_block);
      }
      return ret;
    }

    int BlockFileManager::switch_compact_blk(const uint32_t block_id)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      LogicBlockMapIter mit = compact_logic_blocks_.find(block_id);
      if (mit == compact_logic_blocks_.end())
      {
        return EXIT_COMPACT_BLOCK_ERROR;
      }

      LogicBlockMapIter fit = logic_blocks_.find(block_id);
      if (fit == logic_blocks_.end())
      {
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      LogicBlock *cpt_logic_block = mit->second;
      LogicBlock *old_logic_block = fit->second;

      // switch
      logic_blocks_[block_id] = cpt_logic_block;
      compact_logic_blocks_[block_id] = old_logic_block;

      return TFS_SUCCESS;
    }

    int BlockFileManager::expire_compact_blk(const time_t time, std::set<uint32_t>& erase_blocks)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      erase_blocks.clear();
      int32_t old_compact_block_size = compact_logic_blocks_.size();
      for (LogicBlockMapIter mit = compact_logic_blocks_.begin(); mit != compact_logic_blocks_.end();)
      {
        if (time < 0)
          break;

        // get last update
        if ((mit->second) && (mit->second->get_last_update() < time))
        {
          TBSYS_LOG(INFO, "compact block is expired. blockid: %u, now: %ld, last update: %ld", mit->first, time,
                    mit->second->get_last_update());
          // add to erase block
          erase_blocks.insert(mit->first);
        }

        ++mit;
      }

      int32_t new_compact_block_size = compact_logic_blocks_.size() - erase_blocks.size();
      TBSYS_LOG(INFO, "compact block map: old: %d, new: %d", old_compact_block_size, new_compact_block_size);
      return TFS_SUCCESS;
    }

    int BlockFileManager::set_error_bitmap(const std::set<uint32_t>& error_blocks)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      // batch set error block bitmap
      for (std::set<uint32_t>::iterator sit = error_blocks.begin(); sit != error_blocks.end(); ++sit)
      {
        TBSYS_LOG(INFO, "set error bitmap, pos: %d", *sit);
        error_bit_map_->set(*sit);
      }
      return TFS_SUCCESS;
    }

    int BlockFileManager::reset_error_bitmap(const std::set<uint32_t>& reset_error_blocks)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      // batch reset error block bitmap
      for (std::set<uint32_t>::iterator sit = reset_error_blocks.begin(); sit != reset_error_blocks.end(); ++sit)
      {
        TBSYS_LOG(INFO, "reset error bitmap, pos: %d", *sit);
        error_bit_map_->reset(*sit);
      }
      return TFS_SUCCESS;
    }

    LogicBlock* BlockFileManager::get_logic_block(const uint32_t logic_block_id, const BlockType block_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      LogicBlockMapIter mit;
      if (C_COMPACT_BLOCK == block_type)
      {
        TBSYS_LOG(DEBUG, "get compact block. logic blockid: %u.", logic_block_id);
        mit = compact_logic_blocks_.find(logic_block_id);
        if (mit == compact_logic_blocks_.end())
        {
          return NULL;
        }
      }
      else                      // main block
      {
        mit = logic_blocks_.find(logic_block_id);
        if (mit == logic_blocks_.end())
        {
          return NULL;
        }
      }

      // update visit count
      mit->second->add_visit_count();
      return mit->second;
    }

    int BlockFileManager::get_all_logic_block(std::list<LogicBlock*>& logic_block_list, const BlockType block_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      logic_block_list.clear();
      if (C_MAIN_BLOCK == block_type)
      {
        for (LogicBlockMapIter mit = logic_blocks_.begin(); mit != logic_blocks_.end(); ++mit)
        {
          logic_block_list.push_back(mit->second);
        }
      }
      else                      // compact logic blocks
      {
        for (LogicBlockMapIter mit = compact_logic_blocks_.begin(); mit != compact_logic_blocks_.end(); ++mit)
        {
          logic_block_list.push_back(mit->second);
        }
      }
      return TFS_SUCCESS;
    }

    int BlockFileManager::get_all_block_info(std::set<common::BlockInfo>& blocks, const BlockType block_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      blocks.clear();
      if (C_MAIN_BLOCK == block_type)
      {
        for (LogicBlockMapIter mit = logic_blocks_.begin(); mit != logic_blocks_.end(); ++mit)
        {
          blocks.insert(*mit->second->get_block_info());
        }
      }
      else                      // compact logic blocks
      {
        for (LogicBlockMapIter mit = compact_logic_blocks_.begin(); mit != compact_logic_blocks_.end(); ++mit)
        {
          blocks.insert(*mit->second->get_block_info());
        }
      }
      return TFS_SUCCESS;
    }


    int64_t BlockFileManager::get_all_logic_block_size(const BlockType block_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      return C_MAIN_BLOCK == block_type ? logic_blocks_.size() : compact_logic_blocks_.size();
    }

    int BlockFileManager::get_logic_block_ids(VUINT& logic_block_ids, const BlockType block_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      logic_block_ids.clear();
      if (C_MAIN_BLOCK == block_type)
      {
        for (LogicBlockMapIter mit = logic_blocks_.begin(); mit != logic_blocks_.end(); ++mit)
        {
          logic_block_ids.push_back(mit->first);
        }
      }
      else
      {
        for (LogicBlockMapIter mit = compact_logic_blocks_.begin(); mit != compact_logic_blocks_.end(); ++mit)
        {
          logic_block_ids.push_back(mit->first);
        }
      }
      return TFS_SUCCESS;
    }

    int BlockFileManager::get_all_physic_block(list<PhysicalBlock*>& physic_block_list)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      physic_block_list.clear();
      for (PhysicalBlockMapIter mit = physical_blocks_.begin(); mit != physical_blocks_.end(); ++mit)
      {
        physic_block_list.push_back(mit->second);
      }
      return TFS_SUCCESS;
    }

    int BlockFileManager::query_super_block(SuperBlock& super_block_info)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      super_block_info = super_block_;
      return TFS_SUCCESS;
    }

    int BlockFileManager::query_bit_map(char** bit_map_buffer, int32_t& bit_map_len, int32_t& set_count,
                                        const BitMapType bitmap_type)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      bit_map_len = bit_map_size_;
      *bit_map_buffer = new char[bit_map_size_];
      if (C_ALLOCATE_BLOCK == bitmap_type)
      {
        set_count = normal_bit_map_->get_set_count();
        memcpy(*bit_map_buffer, normal_bit_map_->get_data(), bit_map_size_);
      }
      else
      {
        set_count = error_bit_map_->get_set_count();
        memcpy(*bit_map_buffer, error_bit_map_->get_data(), bit_map_size_);
      }
      return TFS_SUCCESS;
    }

    int BlockFileManager::query_approx_block_count(int32_t& block_count) const
    {
      // use super block recorded count, just log confict
      if (super_block_.used_block_count_ != static_cast<int32_t> (logic_blocks_.size()))
      {
        TBSYS_LOG(WARN, "conflict! used main block: %zd, super main block: %u", logic_blocks_.size(),
                  super_block_.used_block_count_);
      }

      // modify ratio
      int32_t approx_block_count = static_cast<int32_t> (super_block_.used_extend_block_count_
                                                         * super_block_.block_type_ratio_);

      // hardcode 1.1 approx radio
      if (super_block_.used_block_count_ * 1.1 < approx_block_count)
      {
        TBSYS_LOG(DEBUG, "used mainblock: %u, used approx block: %u", super_block_.used_block_count_,
                  approx_block_count);
        block_count = approx_block_count;
        return TFS_SUCCESS;
      }

      block_count = super_block_.used_block_count_;
      return TFS_SUCCESS;
    }

    int BlockFileManager::query_space(int64_t& used_bytes, int64_t& total_bytes) const
    {
      int64_t used_main_bytes = static_cast<int64_t> (super_block_.used_block_count_)
        * static_cast<int64_t> (super_block_.main_block_size_) + static_cast<int64_t> (super_block_.used_block_count_
                                                                                       / super_block_.block_type_ratio_) * static_cast<int64_t> (super_block_.extend_block_size_);

      int64_t used_ext_bytes = static_cast<int64_t> (super_block_.used_extend_block_count_)
        * static_cast<int64_t> (super_block_.extend_block_size_)
        + static_cast<int64_t> (super_block_.used_extend_block_count_ * super_block_.block_type_ratio_)
        * static_cast<int64_t> (super_block_.main_block_size_);

      if (static_cast<int64_t> (used_main_bytes * 1.1) < used_ext_bytes)
      {
        TBSYS_LOG(DEBUG, "used main block: %d, used ext block: %d\n", super_block_.used_block_count_,
                  super_block_.used_extend_block_count_);
        used_bytes = used_ext_bytes;
      }
      else                      // just consider used main bytes
      {
        used_bytes = used_main_bytes;
      }

      total_bytes = static_cast<int64_t> (super_block_.main_block_count_)
        * static_cast<int64_t> (super_block_.main_block_size_)
        + static_cast<int64_t> (super_block_.extend_block_count_)
        * static_cast<int64_t> (super_block_.extend_block_size_);
      return TFS_SUCCESS;
    }

    int BlockFileManager::load_super_blk(const FileSystemParameter& fs_param)
    {
      bool fs_init_status = true;

      TBSYS_LOG(INFO, "read super block. mount name: %s, offset: %d\n", fs_param.mount_name_.c_str(),
                fs_param.super_block_reserve_offset_);
      super_block_impl_ = new SuperBlockImpl(fs_param.mount_name_, fs_param.super_block_reserve_offset_);
      int ret = super_block_impl_->read_super_blk(super_block_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read super block error. ret: %d, desc: %s\n", ret, strerror(errno));
        return ret;
      }

      // return false
      if (!super_block_impl_->check_status(DEV_TAG, super_block_))
      {
        fs_init_status = false;
      }

      if (!fs_init_status)
      {
        TBSYS_LOG(ERROR, "file system not inited. please format it first.\n");
        return EXIT_FS_NOTINIT_ERROR;
      }

      if (fs_param.mount_name_.compare(super_block_.mount_point_) != 0)
      {
        TBSYS_LOG(WARN, "mount point conflict, rewrite mount point. former: %s, now: %s.\n",
            super_block_.mount_point_, fs_param.mount_name_.c_str());
        strncpy(super_block_.mount_point_, fs_param.mount_name_.c_str(), MAX_DEV_NAME_LEN - 1);
        TBSYS_LOG(INFO, "super block mount point: %s.", super_block_.mount_point_);
        super_block_impl_->write_super_blk(super_block_);
        super_block_impl_->flush_file();
      }

      unsigned item_count = super_block_.main_block_count_ + super_block_.extend_block_count_ + 1;
      TBSYS_LOG(INFO, "file system bitmap size: %u\n", item_count);
      normal_bit_map_ = new BitMap(item_count);
      error_bit_map_ = new BitMap(item_count);

      // load bitmap
      bit_map_size_ = normal_bit_map_->get_slot_count();
      char* tmp_bit_map_buf = new char[4 * bit_map_size_];
      memset(tmp_bit_map_buf, 0, 4 * bit_map_size_);

      ret = super_block_impl_->read_bit_map(tmp_bit_map_buf, 4 * bit_map_size_);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDeleteA(tmp_bit_map_buf);
        return ret;
      }

      // compare two bitmap
      if (memcmp(tmp_bit_map_buf, tmp_bit_map_buf + bit_map_size_, bit_map_size_) != 0)
      {
        tbsys::gDeleteA(tmp_bit_map_buf);
        TBSYS_LOG(ERROR, "normal bitmap conflict");
        return EXIT_BITMAP_CONFLICT_ERROR;
      }

      if (memcmp(tmp_bit_map_buf + 2 * bit_map_size_, tmp_bit_map_buf + 3 * bit_map_size_, bit_map_size_) != 0)
      {
        tbsys::gDeleteA(tmp_bit_map_buf);
        TBSYS_LOG(ERROR, "error bitmap conflict");
        return EXIT_BITMAP_CONFLICT_ERROR;
      }

      //load bitmap
      normal_bit_map_->copy(bit_map_size_, tmp_bit_map_buf);
      error_bit_map_->copy(bit_map_size_, tmp_bit_map_buf + 2 * bit_map_size_);
      TBSYS_LOG(INFO, "bitmap used count: %u, error: %u", normal_bit_map_->get_set_count(),
                error_bit_map_->get_set_count());
      tbsys::gDeleteA(tmp_bit_map_buf);

      return TFS_SUCCESS;
    }

    int BlockFileManager::load_block_file()
    {
      // construct logic block and corresponding physicblock list according to bitmap
      PhysicalBlockMapIter pit;
      bool conflict_flag = false;
      PhysicalBlock* t_physical_block = NULL;
      PhysicalBlock* ext_physical_block = NULL;
      LogicBlock* t_logic_block = NULL;
      int ret = TFS_SUCCESS;
      // traverse bitmap
      for (uint32_t pos = 1; pos <= static_cast<uint32_t> (super_block_.main_block_count_); ++pos)
      {
        // init every loop
        t_physical_block = NULL;
        t_logic_block = NULL;
        ext_physical_block = NULL;

        // 1. test bitmap find
        if (normal_bit_map_->test(pos))
        {
          // 2. construct new physic block
          t_physical_block = new PhysicalBlock(pos, super_block_.mount_point_, super_block_.main_block_size_,
                                               C_MAIN_BLOCK);
          // 3. read physical block's head,check prefix
          ret = t_physical_block->load_block_prefix();
          if (TFS_SUCCESS != ret)
          {
            tbsys::gDelete(t_physical_block);
            TBSYS_LOG(ERROR, "init physical block fail. fatal error! pos: %d, file: %s", pos, super_block_.mount_point_);
            break;
          }

          BlockPrefix block_prefix;
          t_physical_block->get_block_prefix(block_prefix);
          // not inited, error
          if (0 == block_prefix.logic_blockid_)
          {
            tbsys::gDelete(t_physical_block);
            TBSYS_LOG(ERROR, "block prefix illegal! logic blockid: %u, physical pos: %u.", block_prefix.logic_blockid_,
                      pos);
            normal_bit_map_->reset(pos);

            // write or flush fail, just log, not exit. not dirty global ret value
            int tmp_ret;
            if ((tmp_ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_)) != TFS_SUCCESS)
              TBSYS_LOG(ERROR, "write bit map fail %d", tmp_ret);
            if ((tmp_ret = super_block_impl_->flush_file()) != TFS_SUCCESS)
              TBSYS_LOG(ERROR, "flush super block fail %d", tmp_ret);

            continue;
          }

          // 4. construct logic block, add physic block
          t_logic_block = new LogicBlock(block_prefix.logic_blockid_, pos, super_block_.mount_point_);
          t_logic_block->add_physic_block(t_physical_block);

          // record physical block id
          uint32_t logic_block_id = block_prefix.logic_blockid_;
          TBSYS_LOG(
            INFO,
            "load logic block. logic blockid: %u, physic prev blockid: %u, physic next blockid: %u, physical blockid: %u.",
            logic_block_id, block_prefix.prev_physic_blockid_,
            block_prefix.next_physic_blockid_, pos);

          // 5. make sure this physical block hasn't been loaded
          pit = physical_blocks_.find(pos);
          if (pit != physical_blocks_.end())
          {
            tbsys::gDelete(t_physical_block);
            tbsys::gDelete(t_logic_block);
            ret = EXIT_PHYSIC_UNEXPECT_FOUND_ERROR;
            TBSYS_LOG(ERROR, "logic blockid: %u, physical blockid: %u is repetitive. fatal error! ret: %d",
                      logic_block_id, pos, EXIT_PHYSIC_UNEXPECT_FOUND_ERROR);
            break;
          }

          // 6. find if it is a existed logic block.
          // if exist, delete the exist one(this scene is appear in the compact process: server down after set clean),
          LogicBlockMapIter mit = logic_blocks_.find(logic_block_id);
          if (mit != logic_blocks_.end())
          {
            TBSYS_LOG(INFO, "logic blockid: %u, map blockid: %u already exist", logic_block_id, mit->first);
            mit = compact_logic_blocks_.find(logic_block_id);
            if (mit != compact_logic_blocks_.end())
            {
              TBSYS_LOG(INFO, "logic blockid: %u already exist in compact block map", logic_block_id);
              ret = del_block(logic_block_id, C_COMPACT_BLOCK);
              if (TFS_SUCCESS != ret)
              {
                tbsys::gDelete(t_physical_block);
                tbsys::gDelete(t_logic_block);
                break;
              }
            }
            compact_logic_blocks_.insert(LogicBlockMap::value_type(logic_block_id, t_logic_block));
          }
          else                  // not exist, insert
          {
            logic_blocks_.insert(LogicBlockMap::value_type(logic_block_id, t_logic_block));
          }

          // 7. insert physic block to physic map
          physical_blocks_.insert(PhysicalBlockMap::value_type(pos, t_physical_block));

          uint32_t ext_pos = pos;
          // 8. add extend physical block
          while (0 != block_prefix.next_physic_blockid_)
          {
            ext_physical_block = NULL;
            uint32_t prev_block_id = ext_pos;
            ext_pos = block_prefix.next_physic_blockid_;

            //bitmap is not set
            //program exit
            if (!normal_bit_map_->test(ext_pos))
            {
              TBSYS_LOG(ERROR, "ext next physicblock and bitmap conflicted! logic blockid: %u, physical blockid: %u.",
                        logic_block_id, ext_pos);
              ret = EXIT_BLOCKID_CONFLICT_ERROR;
              break;
            }

            ext_physical_block = new PhysicalBlock(ext_pos, super_block_.mount_point_,
                                                   super_block_.extend_block_size_, C_EXT_BLOCK);
            ret = ext_physical_block->load_block_prefix();
            if (TFS_SUCCESS != ret)
            {
              tbsys::gDelete(ext_physical_block);
              TBSYS_LOG(ERROR, "init physical block fail. fatal error! pos: %u, mount point: %s", pos,
                        super_block_.mount_point_);
              break;
            }

            memset(&block_prefix, 0, sizeof(BlockPrefix));
            ext_physical_block->get_block_prefix(block_prefix);
            if (prev_block_id != block_prefix.prev_physic_blockid_)
            {
              TBSYS_LOG(ERROR, "read prev blockid conflict! prev blockid: %u. block prefix's physic prev blockid: %u",
                        prev_block_id, block_prefix.prev_physic_blockid_);
              //release
              normal_bit_map_->reset(pos);


            // write or flush fail, just log, not exit. not dirty global ret value
              int tmp_ret;
              if ((tmp_ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_)) != TFS_SUCCESS)
                TBSYS_LOG(ERROR, "write bit map fail %d", tmp_ret);
              if ((tmp_ret = super_block_impl_->flush_file()) != TFS_SUCCESS)
                TBSYS_LOG(ERROR, "flush super block fail %d", tmp_ret);
              conflict_flag = true;
              tbsys::gDelete(ext_physical_block);
              break;
            }

            TBSYS_LOG(
              INFO,
              "read blockprefix! ext physical blockid pos: %u. prev blockid: %u. blockprefix's physic prev blockid: %u, next physic blockid: %u, logic blockid :%u",
              ext_pos, prev_block_id, block_prefix.prev_physic_blockid_, block_prefix.next_physic_blockid_,
              block_prefix.logic_blockid_);
            pit = physical_blocks_.find(ext_pos);
            if (pit != physical_blocks_.end())
            {
              tbsys::gDelete(ext_physical_block);
              ret = EXIT_PHYSIC_UNEXPECT_FOUND_ERROR;
              TBSYS_LOG(ERROR, "logic blockid: %u, physical blockid: %u is repetitive. fatal error!", logic_block_id,
                        ext_pos);
              tbsys::gDelete(ext_physical_block);
              break;
            }

            t_logic_block->add_physic_block(ext_physical_block);
            physical_blocks_.insert(PhysicalBlockMap::value_type(ext_pos, ext_physical_block));
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          if (conflict_flag)
          {
            TBSYS_LOG(ERROR, "delete load conflict block. logic blockid: %u", logic_block_id);
            del_block(logic_block_id, C_CONFUSE_BLOCK);
            conflict_flag = false;
            continue;
          }

          // 9. load logic block
          ret = t_logic_block->load_block_file(super_block_.hash_slot_size_, super_block_.mmap_option_);
          if (TFS_SUCCESS != ret)
          {
            // can not make sure the type of this block, so add to confuse type
            if (EXIT_COMPACT_BLOCK_ERROR == ret || EXIT_BLOCKID_ZERO_ERROR == ret ||
                EXIT_INDEX_CORRUPT_ERROR == ret || EXIT_HALF_BLOCK_ERROR == ret)
            {
              TBSYS_LOG(WARN, "logicblock status abnormal, need delete! blockid: %u. ret: %d", logic_block_id, ret);
              del_block(logic_block_id, C_CONFUSE_BLOCK);
              ret = TFS_SUCCESS;
            }
            else
            {
              // if these error happened when load block, program should exit
              TBSYS_LOG(ERROR, "logicblock load error! logic blockid: %u. ret: %d", logic_block_id, ret);
              break;
            }
          }
        }

      }

      return ret;
    }

    int BlockFileManager::find_avail_block(uint32_t& ext_physical_block_id, const BlockType block_type)
    {
      int32_t i = 1;
      int32_t size = super_block_.main_block_count_;
      if (C_EXT_BLOCK == block_type)
      {
        i = super_block_.main_block_count_ + 1;
        size += super_block_.extend_block_count_;
      }

      bool hit_block = false;
      for (; i <= size; ++i)
      {
        //find first avaiable block
        if (!normal_bit_map_->test(i))
        {
          //test error bitmap
          if (error_bit_map_->test(i)) //skip error block
          {
            continue;
          }

          hit_block = true;
          break;
        }
      }

      // find nothing
      if (!hit_block)
      {
        TBSYS_LOG(ERROR, "block is exhausted! blocktype: %d\n", block_type);
        return EXIT_BLOCK_EXHAUST_ERROR;
      }

      TBSYS_LOG(DEBUG, "find avail blockid: %u\n", i);
      ext_physical_block_id = i;
      return TFS_SUCCESS;
    }

    int BlockFileManager::init_super_blk_param(const FileSystemParameter& fs_param)
    {
      memset((void *) &super_block_, 0, sizeof(SuperBlock));
      memcpy(super_block_.mount_tag_, DEV_TAG, sizeof(super_block_.mount_tag_));
      super_block_.time_ = time(NULL);
      strncpy(super_block_.mount_point_, fs_param.mount_name_.c_str(), MAX_DEV_NAME_LEN - 1);
      TBSYS_LOG(INFO, "super block mount point: %s.", super_block_.mount_point_);
      int32_t scale = 1024;
      super_block_.mount_point_use_space_ = fs_param.max_mount_size_ * scale;
      super_block_.base_fs_type_ = static_cast<BaseFsType> (fs_param.base_fs_type_);

      if (EXT4 != super_block_.base_fs_type_ && EXT3_FULL != super_block_.base_fs_type_ && EXT3_FTRUN
          != super_block_.base_fs_type_)
      {
        TBSYS_LOG(ERROR, "base fs type is not supported. base fs type: %d", super_block_.base_fs_type_);
        return TFS_ERROR;
      }

      super_block_.superblock_reserve_offset_ = fs_param.super_block_reserve_offset_;
      // leave sizeof(int)
      super_block_.bitmap_start_offset_ = super_block_.superblock_reserve_offset_ + 2 * sizeof(SuperBlock)
          + sizeof(int32_t);
      super_block_.avg_segment_size_ = fs_param.avg_segment_size_;
      super_block_.block_type_ratio_ = fs_param.block_type_ratio_;

      int32_t data_ratio = super_block_.avg_segment_size_ / (META_INFO_SIZE * INDEXFILE_SAFE_MULT);

      int64_t avail_data_space = static_cast<int64_t> (super_block_.mount_point_use_space_
          * (static_cast<float> (data_ratio) / static_cast<float> (data_ratio + 1)));
      if (avail_data_space <= 0)
      {
        TBSYS_LOG(
            ERROR,
            "format filesystem fail. avail data space: %" PRI64_PREFIX "d, avg segment size: %d, single meta size: %u, data ratio: %d\n",
            avail_data_space, super_block_.avg_segment_size_, META_INFO_SIZE, data_ratio);
        return TFS_ERROR;
      }

      super_block_.main_block_size_ = fs_param.main_block_size_;
      super_block_.extend_block_size_ = fs_param.extend_block_size_;

      int32_t main_block_count = 0, extend_block_count = 0;
      calc_block_count(avail_data_space, main_block_count, extend_block_count);
      super_block_.main_block_count_ = main_block_count;
      super_block_.extend_block_count_ = extend_block_count;

      super_block_.used_block_count_ = 0;
      super_block_.used_extend_block_count_ = 0;
      super_block_.hash_slot_ratio_ = fs_param.hash_slot_ratio_;

      //add for configure file
      int32_t per_block_file_num = static_cast<int32_t> ((super_block_.main_block_size_
          + static_cast<float> (super_block_.extend_block_size_) / fs_param.block_type_ratio_)
          / super_block_.avg_segment_size_);
      // bucket / file =  hash_slot_ratio_
      int32_t hash_bucket_size = static_cast<int32_t> (super_block_.hash_slot_ratio_
          * static_cast<float> (per_block_file_num));
      int32_t hash_bucket_mem_size = hash_bucket_size * sizeof(int32_t);
      int32_t meta_info_size = META_INFO_SIZE * per_block_file_num;
      int32_t need_mmap_size = hash_bucket_mem_size + meta_info_size;
      super_block_.hash_slot_size_ = hash_bucket_size;

      // mmap page size
      int32_t sz = getpagesize();
      int32_t count = need_mmap_size / sz;
      int32_t remainder = need_mmap_size % sz;
      MMapOption mmap_option;
      mmap_option.first_mmap_size_ = remainder ? (count + 1) * sz : count * sz;
      mmap_option.per_mmap_size_ = sz;
      mmap_option.max_mmap_size_ = mmap_option.first_mmap_size_ * INNERFILE_MAX_MULTIPE;

      super_block_.mmap_option_ = mmap_option;
      super_block_.version_ = fs_param.file_system_version_;

      super_block_.display();
      return TFS_SUCCESS;
    }

    void BlockFileManager::calc_block_count(const int64_t avail_data_space, int32_t& main_block_count,
        int32_t& ext_block_count)
    {
      ext_block_count = static_cast<int32_t> (static_cast<float> (avail_data_space) / (super_block_.block_type_ratio_
          * static_cast<float> (super_block_.main_block_size_) + static_cast<float> (super_block_.extend_block_size_)));
      main_block_count = static_cast<int32_t> (static_cast<float> (ext_block_count) * super_block_.block_type_ratio_);
      TBSYS_LOG(INFO,
          "cal block count. avail data space: %" PRI64_PREFIX "d, main block count: %d, ext block count: %d",
          avail_data_space, main_block_count, ext_block_count);
    }

    int BlockFileManager::create_fs_dir()
    {
      //super_block_.display();
      int ret = mkdir(super_block_.mount_point_, DIR_MODE);
      if (ret && errno != EEXIST)
      {
        TBSYS_LOG(ERROR, "make extend dir error. dir: %s, ret: %d, error: %d, error desc: %s",
            super_block_.mount_point_, ret, errno, strerror(errno));
        return TFS_ERROR;
      }

      // extend block directory
      std::string extend_dir = super_block_.mount_point_;
      extend_dir += EXTENDBLOCK_DIR_PREFIX;
      ret = mkdir(extend_dir.c_str(), DIR_MODE);
      if (ret)
      {
        TBSYS_LOG(ERROR, "make extend dir:%s error. ret: %d, error: %d", extend_dir.c_str(), ret, errno);
        return TFS_ERROR;
      }

      // index file directory
      std::string index_dir = super_block_.mount_point_;
      index_dir += INDEX_DIR_PREFIX;
      ret = mkdir(index_dir.c_str(), DIR_MODE);
      if (ret)
      {
        TBSYS_LOG(ERROR, "make index dir error. ret: %d, error: %d", ret, errno);
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    uint32_t BlockFileManager::calc_bitmap_count()
    {
      uint32_t item_count = super_block_.main_block_count_ + super_block_.extend_block_count_ + 1;
      BitMap tmp_bit_map(item_count);
      uint32_t slot_count = tmp_bit_map.get_slot_count();
      TBSYS_LOG(INFO, "cal bitmap count. item count: %u, slot count: %u", item_count, slot_count);
      return slot_count;
    }

    int BlockFileManager::create_fs_super_blk()
    {
      uint32_t bit_map_size = calc_bitmap_count();
      int super_block_file_size = 2 * sizeof(SuperBlock) + sizeof(int32_t) + 4 * bit_map_size;

      char* tmp_buffer = new char[super_block_file_size];
      memcpy(tmp_buffer, &super_block_, sizeof(SuperBlock));
      memcpy(tmp_buffer + sizeof(SuperBlock), &super_block_, sizeof(SuperBlock));
      memset(tmp_buffer + 2 * sizeof(SuperBlock), 0, 4 * bit_map_size + sizeof(int));

      std::string super_block_file = super_block_.mount_point_;
      super_block_file += SUPERBLOCK_NAME;
      FileOperation* super_file_op = new FileOperation(super_block_file, O_RDWR | O_CREAT);
      int ret = super_file_op->pwrite_file(tmp_buffer, super_block_file_size, 0);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write super block file error. ret: %d.", ret);
      }

      tbsys::gDelete(super_file_op);
      tbsys::gDeleteA(tmp_buffer);

      return ret;
    }

    int BlockFileManager::create_block(const BlockType block_type)
    {
      int32_t prefix_size = sizeof(BlockPrefix);
      char* block_prefix = new char[prefix_size];
      memset(block_prefix, 0, prefix_size);
      FileFormater* file_formater = NULL;

      if (EXT4 == super_block_.base_fs_type_)
      {
        file_formater = new Ext4FileFormater();
      }
      else if (EXT3_FULL == super_block_.base_fs_type_)
      {
        file_formater = new Ext3FullFileFormater();
      }
      else if (EXT3_FTRUN == super_block_.base_fs_type_)
      {
        file_formater = new Ext3SimpleFileFormater();
      }
      else
      {
        TBSYS_LOG(ERROR, "base fs type is not supported. base fs type: %d", super_block_.base_fs_type_);
        tbsys::gDeleteA(block_prefix);
        return TFS_ERROR;
      }

      int32_t block_count = 0;
      int32_t block_size = 0;
      if (C_MAIN_BLOCK == block_type)
      {
        block_count = super_block_.main_block_count_;
        block_size = super_block_.main_block_size_;
      }
      else if (C_EXT_BLOCK == block_type)
      {
        block_count = super_block_.extend_block_count_;
        block_size = super_block_.extend_block_size_;
      }
      else
      {
        tbsys::gDeleteA(block_prefix);
        tbsys::gDelete(file_formater);
        return TFS_ERROR;
      }

      for (int32_t i = 1; i <= block_count; ++i)
      {
        std::string block_file;
        std::stringstream tmp_stream;
        if (C_MAIN_BLOCK == block_type)
        {
          tmp_stream << super_block_.mount_point_ << MAINBLOCK_DIR_PREFIX << i;
        }
        else
        {
          tmp_stream << super_block_.mount_point_ << EXTENDBLOCK_DIR_PREFIX << (i + super_block_.main_block_count_);
        }
        tmp_stream >> block_file;

        FileOperation* file_op = new FileOperation(block_file, O_RDWR | O_CREAT);
        int ret = file_op->open_file();
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "allocate space error. ret: %d, error: %d, error desc: %s\n", ret, errno, strerror(errno));
          tbsys::gDelete(file_op);
          tbsys::gDeleteA(block_prefix);
          tbsys::gDelete(file_formater);
          return ret;
        }

        ret = file_formater->block_file_format(file_op->get_fd(), block_size);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "allocate space error. ret: %d, error: %d, error desc: %s\n", ret, errno, strerror(errno));
          tbsys::gDelete(file_op);
          tbsys::gDeleteA(block_prefix);
          tbsys::gDelete(file_formater);
          return ret;
        }
        ret = file_op->pwrite_file(block_prefix, prefix_size, 0);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write block file error. physcial blockid: %d, block type: %d, ret: %d.", i, block_type,
              ret);
          tbsys::gDelete(file_op);
          tbsys::gDeleteA(block_prefix);
          tbsys::gDelete(file_formater);
          return ret;
        }
        tbsys::gDelete(file_op);
      }

      tbsys::gDeleteA(block_prefix);
      tbsys::gDelete(file_formater);
      return TFS_SUCCESS;
    }

    int BlockFileManager::create_block_prefix()
    {
      FileFormater* file_formater = NULL;

      if (EXT4 == super_block_.base_fs_type_)
      {
        file_formater = new Ext4FileFormater();
      }
      else if (EXT3_FULL == super_block_.base_fs_type_)
      {
        file_formater = new Ext3FullFileFormater();
      }
      else if (EXT3_FTRUN == super_block_.base_fs_type_)
      {
        file_formater = new Ext3SimpleFileFormater();
      }
      else
      {
        TBSYS_LOG(ERROR, "base fs type is not supported. base fs type: %d", super_block_.base_fs_type_);
        return TFS_ERROR;
      }

      // set block_prefix name
      std::string block_prefix_file;
      block_prefix_file = super_block_.mount_point_ + BLOCK_HEADER_PREFIX;
      int block_count =  super_block_.main_block_count_ + super_block_.extend_block_count_;
      int bp_file_size = block_count * sizeof(BlockPrefix);

      FileOperation* file_op = new FileOperation(block_prefix_file, O_RDWR | O_CREAT);
      int ret = file_op->open_file();
      if (ret < 0)
      {
        tbsys::gDelete(file_op);
        tbsys::gDelete(file_formater);
        TBSYS_LOG(ERROR, "create file error. ret: %d, error: %d, error desc: %s\n", ret, errno, strerror(errno));
        return ret;
      }

      ret = file_formater->block_file_format(file_op->get_fd(), bp_file_size);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(file_op);
        tbsys::gDelete(file_formater);
        TBSYS_LOG(ERROR, "allocate space error. ret: %d, error: %d, error desc: %s\n", ret, errno, strerror(errno));
        return ret;
      }

      // make sure all the bytes set to 0
      int left = bp_file_size;
      int wsize = 10240;
      char* zero_buf = new (std::nothrow) char[wsize];
      if (NULL == zero_buf)
      {
        tbsys::gDelete(file_op);
        tbsys::gDelete(file_formater);
        TBSYS_LOG(ERROR, "allocate space error. ret: %d, error: %d, error desc: %s\n", ret, errno, strerror(errno));
        return EXIT_GENERAL_ERROR;
      }
      memset(zero_buf, 0, wsize);

      while (left > 0)
      {
        wsize = left < wsize? left: wsize;
        ret = file_op->pwrite_file(zero_buf, wsize, bp_file_size - left);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(zero_buf);
          tbsys::gDelete(file_op);
          tbsys::gDelete(file_formater);
          TBSYS_LOG(ERROR, "write block_prefix file error. ret: %d.", ret);
          return ret;
        }
        left -= wsize;
      }

      /* don't set super block version to 2 for upgrade
      SuperBlock super_block;
      SuperBlockImpl* super_block_impl = new SuperBlockImpl(SYSPARAM_FILESYSPARAM.mount_name_,
          SYSPARAM_FILESYSPARAM.super_block_reserve_offset_);
      ret = super_block_impl->read_super_blk(super_block);
      if (TFS_SUCCESS == ret)
      {
        super_block.version_ = FS_SPEEDUP_VERSION;
        ret = super_block_impl->write_super_blk(super_block);
        if (TFS_SUCCESS == ret)
        {
          super_block_impl->flush_file();
        }
      }
      */

      // tbsys::gDelete(super_block_impl);
      tbsys::gDelete(zero_buf);
      tbsys::gDelete(file_op);
      tbsys::gDelete(file_formater);

      return ret;
   }

    LogicBlock* BlockFileManager::choose_del_block(const uint32_t logic_block_id, BlockType& block_type)
    {
      LogicBlockMapIter mit;
      if (C_COMPACT_BLOCK == block_type)
      {
        mit = compact_logic_blocks_.find(logic_block_id);
        if (mit == compact_logic_blocks_.end())
        {
          return NULL;
        }
      }
      else if (C_MAIN_BLOCK == block_type)
      {
        mit = logic_blocks_.find(logic_block_id);
        if (mit == logic_blocks_.end())
        {
          return NULL;
        }
      }
      else if (C_CONFUSE_BLOCK == block_type) // confuse,then try to find in main and compact block map
      {
        mit = compact_logic_blocks_.find(logic_block_id);
        if (mit == compact_logic_blocks_.end())
        {
          mit = logic_blocks_.find(logic_block_id);
          if (mit == logic_blocks_.end())
          {
            return NULL;
          }
          else
          {
            block_type = C_MAIN_BLOCK;
          }
        }
        else
        {
          block_type = C_COMPACT_BLOCK;
        }
      }
      else
      {
        return NULL;
      }

      return mit->second;
    }

    int BlockFileManager::erase_logic_block(const uint32_t logic_block_id, const BlockType block_type)
    {
      if (C_COMPACT_BLOCK == block_type)
      {
        compact_logic_blocks_.erase(logic_block_id);
      }
      else
      {
        logic_blocks_.erase(logic_block_id);
      }

      return TFS_SUCCESS;
    }

    void BlockFileManager::rollback_superblock(const uint32_t physical_block_id, const bool modify_flag,
        BlockType type)
    {
      if (normal_bit_map_->test(physical_block_id))
      {
        normal_bit_map_->reset(physical_block_id);
        int ret = super_block_impl_->write_bit_map(normal_bit_map_, error_bit_map_);
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "write bit map fail. ret: %d, physical blockid: %u", ret, physical_block_id);

        if (modify_flag)
        {
          if (C_MAIN_BLOCK == type)
          {
            --super_block_.used_block_count_;
          }
          else if (C_EXT_BLOCK == type)
          {
            --super_block_.used_extend_block_count_;
          }
          ret = super_block_impl_->write_super_blk(super_block_);
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "write super block fail. ret: %d", ret);
        }

        ret = super_block_impl_->flush_file();
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "flush super block fail. ret: %d", ret);
      }
      return;
    }
  }
}
