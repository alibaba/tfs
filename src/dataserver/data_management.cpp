/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_management.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "data_management.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
#include "visit_stat.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {

    using namespace common;
    using namespace std;

    DataManagement::DataManagement() :
      file_number_(0), last_gc_data_file_time_(0)
    {
    }

    DataManagement::~DataManagement()
    {
    }

    void DataManagement::set_file_number(const uint64_t file_number)
    {
      file_number_ = file_number;
      TBSYS_LOG(INFO, "set file number. file number: %" PRI64_PREFIX "u\n", file_number_);
    }

    int DataManagement::init_block_files(const FileSystemParameter& fs_param)
    {
      int64_t time_start = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "block file load blocks begin. start time: %" PRI64_PREFIX "d\n", time_start);
      // just start up
      int ret = BlockFileManager::get_instance()->bootstrap(fs_param);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockfile manager boot fail! ret: %d\n", ret);
        return ret;
      }
      int64_t time_end = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "block file load blocks end. end time: %" PRI64_PREFIX "d. cost time: %" PRI64_PREFIX "d.",
          time_end, time_end - time_start);
      return TFS_SUCCESS;
    }

    void DataManagement::get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity)
    {
      BlockFileManager::get_instance()->query_approx_block_count(block_count);
      BlockFileManager::get_instance()->query_space(use_capacity, total_capacity);
      return;
    }

    int DataManagement::get_all_logic_block(std::list<LogicBlock*>& logic_block_list)
    {
      return BlockFileManager::get_instance()->get_all_logic_block(logic_block_list);
    }

    int DataManagement::get_all_block_info(std::set<common::BlockInfo>& blocks)
    {
      return BlockFileManager::get_instance()->get_all_block_info(blocks);
    }

    int64_t DataManagement::get_all_logic_block_size()
    {
      return BlockFileManager::get_instance()->get_all_logic_block_size();
    }

    int DataManagement::create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = TFS_SUCCESS;
      if (0 == file_id)
      {
        ret = logic_block->open_write_file(file_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "try getfileid failed. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d.", block_id,
              file_id, ret);
          return ret;
        }
      }
      else                      // update seq no over current one to avoid overwrite
      {
        logic_block->reset_seq_id(file_id);
      }

      data_file_mutex_.lock();
      file_number = ++file_number_;
      data_file_mutex_.unlock();

      TBSYS_LOG(DEBUG, "open write file. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
          block_id, file_id, file_number_);
      return TFS_SUCCESS;
    }

    int DataManagement::write_data(const WriteDataInfo& write_info, const int32_t lease_id, int32_t& version,
        const char* data_buffer, UpdateBlockType& repair)
    {
      TBSYS_LOG(DEBUG,
          "write data. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, lease: %d",
          write_info.block_id_, write_info.file_id_, write_info.file_number_, lease_id);
      //if the first fragment, check version
      if (0 == write_info.offset_)
      {
        LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(write_info.block_id_);
        if (NULL == logic_block)
        {
          TBSYS_LOG(ERROR, "blockid: %u is not exist.", write_info.block_id_);
          return EXIT_NO_LOGICBLOCK_ERROR;
        }

        int ret = logic_block->check_block_version(version, repair);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(DEBUG, "check_block_version error. blockid: %u, ret: %d", write_info.block_id_, ret);
          return ret;
        }
      }

      // write data to DataFile first
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(write_info.file_number_);
      DataFile* datafile = NULL;
      if (bit != data_file_map_.end())
      {
        datafile = bit->second;
      }
      else                      // not found
      {
        // control datafile size
        if (data_file_map_.size() >= static_cast<uint32_t> (SYSPARAM_DATASERVER.max_datafile_nums_))
        {
          TBSYS_LOG(ERROR, "blockid: %u, datafile nums: %zd is large than default.", write_info.block_id_,
              data_file_map_.size());
          data_file_mutex_.unlock();
          return EXIT_DATAFILE_OVERLOAD;
        }

        datafile = new DataFile(write_info.file_number_);
        data_file_map_.insert(DataFileMap::value_type(write_info.file_number_, datafile));
      }

      if (NULL == datafile)
      {
        TBSYS_LOG(ERROR, "datafile is null. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
            write_info.block_id_, write_info.file_id_, write_info.file_number_);
        data_file_mutex_.unlock();
        return EXIT_DATA_FILE_ERROR;
      }
      datafile->set_last_update();
      data_file_mutex_.unlock();

      // write to datafile
      int32_t write_len = datafile->set_data(data_buffer, write_info.length_, write_info.offset_);
      if (write_len != write_info.length_)
      {
        TBSYS_LOG(
            ERROR,
            "Datafile write error. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, req writelen: %d, actual writelen: %d",
            write_info.block_id_, write_info.file_id_, write_info.file_number_, write_info.length_, write_len);
        // clean dirty data
        erase_data_file(write_info.file_number_);
        return EXIT_DATA_FILE_ERROR;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::close_write_file(const CloseFileInfo& colse_file_info, int32_t& write_file_size)
    {
      uint32_t block_id = colse_file_info.block_id_;
      uint64_t file_id = colse_file_info.file_id_;
      uint64_t file_number = colse_file_info.file_number_;
      uint32_t crc = colse_file_info.crc_;
      TBSYS_LOG(DEBUG,
          "close write file, blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, crc: %u",
          block_id, file_id, file_number, crc);

      //find datafile
      DataFile* datafile = NULL;
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(file_number);
      if (bit != data_file_map_.end())
      {
        datafile = bit->second;
      }

      //lease expire
      if (NULL == datafile)
      {
        TBSYS_LOG(ERROR, "Datafile is null. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
            block_id, file_id, file_number);
        data_file_mutex_.unlock();
        return EXIT_DATAFILE_EXPIRE_ERROR;
      }
      datafile->set_last_update();
      datafile->add_ref();
      data_file_mutex_.unlock();

      //compare crc
      uint32_t datafile_crc = datafile->get_crc();
      if (crc != datafile_crc)
      {
        TBSYS_LOG(
            ERROR,
            "Datafile crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, local crc: %u, msg crc: %u",
            block_id, file_id, file_number, datafile_crc, crc);
        datafile->sub_ref();
        erase_data_file(file_number);
        return EXIT_DATA_FILE_ERROR;
      }

      write_file_size = datafile->get_length();
      //find block
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        datafile->sub_ref();
        erase_data_file(file_number);
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      TIMER_START();
      int ret = logic_block->close_write_file(file_id, datafile, datafile_crc);
      if (TFS_SUCCESS != ret)
      {
        datafile->sub_ref();
        erase_data_file(file_number);
        return ret;
      }

      TIMER_END();
      if (TIMER_DURATION() > SYSPARAM_DATASERVER.max_io_warn_time_)
      {
        TBSYS_LOG(WARN, "write file cost time: blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
            block_id, file_id, TIMER_DURATION());
      }

      // success, gc datafile
      // close tmp file, release opened file handle
      // datafile , bit->second point to same thing, once delete
      // bit->second, datafile will be obseleted immediately.
      datafile->sub_ref();
      erase_data_file(file_number);
      return TFS_SUCCESS;
    }

    int DataManagement::erase_data_file(const uint64_t file_number)
    {
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(file_number);
      if (bit != data_file_map_.end() && NULL != bit->second && bit->second->get_ref() <= 0)
      {
        tbsys::gDelete(bit->second);
        data_file_map_.erase(bit);
      }
      data_file_mutex_.unlock();
      return TFS_SUCCESS;
    }

    int DataManagement::read_data(const uint32_t block_id, const uint64_t file_id, const int32_t read_offset, const int8_t flag,
        int32_t& real_read_len, char* tmp_data_buffer)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block not exist, blockid: %u", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int64_t start = tbsys::CTimeUtil::getTime();
      int ret = logic_block->read_file(file_id, tmp_data_buffer, real_read_len, read_offset, flag);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read data error, fileid: %" PRI64_PREFIX "u, size: %d, offset: %d, ret: %d",
            block_id, file_id, real_read_len, read_offset, ret);
        return ret;
      }

      TBSYS_LOG(DEBUG, "blockid: %u read data, fileid: %" PRI64_PREFIX "u, read size: %d, offset: %d", block_id,
          file_id, real_read_len, read_offset);

      int64_t end = tbsys::CTimeUtil::getTime();
      if (end - start > SYSPARAM_DATASERVER.max_io_warn_time_)
      {
        TBSYS_LOG(WARN, "read file cost time: blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
            block_id, file_id, end - start);
      }

      return TFS_SUCCESS;
    }

    int DataManagement::read_raw_data(const uint32_t block_id, const int32_t read_offset, int32_t& real_read_len,
        char* tmp_data_buffer)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block not exist, blockid: %u", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->read_raw_data(tmp_data_buffer, real_read_len, read_offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read data batch error, offset: %d, rlen: %d, ret: %d", block_id, read_offset,
            real_read_len, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::read_file_info(const uint32_t block_id, const uint64_t file_id, const int32_t mode,
        FileInfo& finfo)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->read_file_info(file_id, finfo);
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }

      // if mode is 0 and file is not in nomal status, return error.
      if ((0 == finfo.id_)
          || (finfo.id_ != file_id )
          || ((finfo.flag_ & (FI_DELETED | FI_INVALID | FI_CONCEAL)) != 0 && NORMAL_STAT == mode))
      {
        TBSYS_LOG(WARN,
            "FileInfo parse fail. blockid: %u, fileid: %" PRI64_PREFIX "u, infoid: %" PRI64_PREFIX "u, flag: %d",
            block_id, file_id, finfo.id_, finfo.flag_);
        return EXIT_FILE_STATUS_ERROR;
      }

      // minus the header(FileInfo)
      finfo.size_ -= sizeof(FileInfo);
      return TFS_SUCCESS;
    }

    int DataManagement::rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id)
    {
      // return if fileid is same
      if (file_id == new_file_id)
      {
        TBSYS_LOG(WARN, "rename file fail. blockid:%u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u",
            block_id, file_id, new_file_id);
        return EXIT_RENAME_FILEID_SAME_ERROR;
      }

      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->rename_file(file_id, new_file_id);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR,
            "modfileid fail, blockid: %u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u, ret: %d",
            block_id, file_id, new_file_id, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::unlink_file(const uint32_t block_id, const uint64_t file_id, const int32_t action, int64_t& file_size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->unlink_file(file_id, action, file_size);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "del file fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", block_id, file_id, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::batch_new_block(const VUINT32* new_blocks)
    {
      int ret = TFS_SUCCESS;
      if (NULL != new_blocks)
      {
        for (uint32_t i = 0; i < new_blocks->size(); ++i)
        {
          TBSYS_LOG(INFO, "new block: %u\n", new_blocks->at(i));
          uint32_t physic_block_id = 0;
          ret = BlockFileManager::get_instance()->new_block(new_blocks->at(i), physic_block_id);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "new block fail, blockid: %u, ret: %d", new_blocks->at(i), ret);
            return ret;
          }
          else
          {
            TBSYS_LOG(INFO, "new block successful, blockid: %u, phyical blockid: %u", new_blocks->at(i),
                physic_block_id);
          }
        }
      }

      return ret;
    }

    int DataManagement::batch_remove_block(const VUINT32* remove_blocks)
    {
      int ret = TFS_SUCCESS;
      if (NULL != remove_blocks)
      {
        for (uint32_t i = 0; i < remove_blocks->size(); ++i)
        {
          TBSYS_LOG(INFO, "remove block: %u\n", remove_blocks->at(i));
          ret = BlockFileManager::get_instance()->del_block(remove_blocks->at(i));
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "remove block error, blockid: %u, ret: %d", remove_blocks->at(i), ret);
            return ret;
          }
          else
          {
            TBSYS_LOG(INFO, "remove block successful, blockid: %u", remove_blocks->at(i));
          }
        }
      }

      return ret;
    }

    int DataManagement::query_bit_map(const int32_t query_type, char** tmp_data_buffer, int32_t& bit_map_len,
        int32_t& set_count)
    {
      // the caller should release the tmp_data_buffer memory
      if (NORMAL_BIT_MAP == query_type)
      {
        BlockFileManager::get_instance()->query_bit_map(tmp_data_buffer, bit_map_len, set_count, C_ALLOCATE_BLOCK);
      }
      else
      {
        BlockFileManager::get_instance()->query_bit_map(tmp_data_buffer, bit_map_len, set_count, C_ERROR_BLOCK);
      }

      return TFS_SUCCESS;
    }

    int DataManagement::query_block_status(const int32_t query_type, VUINT& block_ids, std::map<uint32_t, std::vector<
        uint32_t> >& logic_2_physic_blocks, std::map<uint32_t, BlockInfo*>& block_2_info)
    {
      std::list<LogicBlock*> logic_blocks;
      std::list<LogicBlock*>::iterator lit;

      BlockFileManager::get_instance()->get_logic_block_ids(block_ids);
      BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);

      if (query_type & LB_PAIRS) // logick block ==> physic block list
      {
        std::list<PhysicalBlock*>* phy_blocks;
        std::list<PhysicalBlock*>::iterator pit;

        for (lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
        {
          if (*lit)
          {
            TBSYS_LOG(DEBUG, "query block status, query type: %d, blockid: %u\n", query_type,
                (*lit)->get_logic_block_id());
            phy_blocks = (*lit)->get_physic_block_list();
            std::vector < uint32_t > phy_block_ids;
            for (pit = phy_blocks->begin(); pit != phy_blocks->end(); ++pit)
            {
              phy_block_ids.push_back((*pit)->get_physic_block_id());
            }

            logic_2_physic_blocks.insert(std::map<uint32_t, std::vector<uint32_t> >::value_type(
                (*lit)->get_logic_block_id(), phy_block_ids));
          }
        }
      }

      if (query_type & LB_INFOS) // logic block info
      {
        for (lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
        {
          if (*lit)
          {
            block_2_info.insert(std::map<uint32_t, BlockInfo*>::value_type((*lit)->get_logic_block_id(),
                (*lit)->get_block_info()));
          }
        }
      }

      return TFS_SUCCESS;
    }

    int DataManagement::get_block_info(const uint32_t block_id, BlockInfo*& blk, int32_t& visit_count)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      blk = logic_block->get_block_info();
      visit_count = logic_block->get_visit_count();
      return TFS_SUCCESS;
    }

    int DataManagement::get_visit_sorted_blockids(std::vector<LogicBlock*>& block_ptrs)
    {
      std::list<LogicBlock*> logic_blocks;
      BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);

      for (std::list<LogicBlock*>::iterator lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
      {
        block_ptrs.push_back(*lit);
      }

      sort(block_ptrs.begin(), block_ptrs.end(), visit_count_sort());
      return TFS_SUCCESS;
    }

    int DataManagement::get_block_file_list(const uint32_t block_id, std::vector<FileInfo>& fileinfos)
    {
      TBSYS_LOG(INFO, "getfilelist. blockid: %u\n", block_id);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->get_file_infos(fileinfos);
      TBSYS_LOG(INFO, "getfilelist. blockid: %u, filenum: %zd\n", block_id, fileinfos.size());
      return TFS_SUCCESS;
    }

    int DataManagement::get_block_meta_info(const uint32_t block_id, RawMetaVec& meta_list)
    {
      TBSYS_LOG(INFO, "get raw meta list. blockid: %u\n", block_id);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->get_meta_infos(meta_list);
      TBSYS_LOG(INFO, "get meta list. blockid: %u, filenum: %zd\n", block_id, meta_list.size());
      return TFS_SUCCESS;
    }

    int DataManagement::reset_block_version(const uint32_t block_id)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->reset_block_version();
      TBSYS_LOG(INFO, "reset block version: %u\n", block_id);
      return TFS_SUCCESS;
    }

    int DataManagement::new_single_block(const uint32_t block_id, const BlockType type)
    {
      int ret = TFS_SUCCESS;
      // delete if exist
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL != logic_block)
      {
        TBSYS_LOG(INFO, "block already exist, blockid: %u. first del it", block_id);
        ret = BlockFileManager::get_instance()->del_block(block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "block already exist, blockid: %u. block delete fail. ret: %d", block_id, ret);
          return ret;
        }
      }

      uint32_t physic_block_id = 0;
      ret = BlockFileManager::get_instance()->new_block(block_id, physic_block_id, type);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "block create error, blockid: %u, ret: %d", block_id, ret);
        return ret;
      }
      return TFS_SUCCESS;
    }

    int DataManagement::del_single_block(const uint32_t block_id)
    {
      TBSYS_LOG(INFO, "remove single block, blockid: %u", block_id);
      return BlockFileManager::get_instance()->del_block(block_id);
    }

    int DataManagement::get_block_curr_size(const uint32_t block_id, int32_t& size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      size = logic_block->get_data_file_size();
      TBSYS_LOG(DEBUG, "blockid: %u data file size: %d\n", block_id, size);
      return TFS_SUCCESS;
    }

    int DataManagement::write_raw_data(const uint32_t block_id, const int32_t data_offset, const int32_t msg_len,
        const char* data_buffer)
    {
      //zero length
      if (0 == msg_len)
      {
        return TFS_SUCCESS;
      }
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      ret = logic_block->write_raw_data(data_buffer, msg_len, data_offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write data batch error. blockid: %u, ret: %d", block_id, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::batch_write_meta(const uint32_t block_id, const BlockInfo* blk, const RawMetaVec* meta_list)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->batch_write_meta(blk, meta_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u batch write meta error.", block_id);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::add_new_expire_block(const VUINT32* expire_block_ids, const VUINT32* remove_block_ids, const VUINT32* new_block_ids)
    {
      // delete expire block
      if (NULL != expire_block_ids)
      {
        TBSYS_LOG(INFO, "expire block list size: %u\n", static_cast<uint32_t>(expire_block_ids->size()));
        for (uint32_t i = 0; i < expire_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "expire(delete) block. blockid: %u\n", expire_block_ids->at(i));
          BlockFileManager::get_instance()->del_block(expire_block_ids->at(i));
        }
      }

      // delete remove block
      if (NULL != remove_block_ids)
      {
        TBSYS_LOG(INFO, "remove block list size: %u\n", static_cast<uint32_t>(remove_block_ids->size()));
        for (uint32_t i = 0; i < remove_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "delete block. blockid: %u\n", remove_block_ids->at(i));
          BlockFileManager::get_instance()->del_block(remove_block_ids->at(i));
        }
      }

      // new
      if (NULL != new_block_ids)
      {
        TBSYS_LOG(INFO, "new block list size: %u\n", static_cast<uint32_t>(new_block_ids->size()));
        for (uint32_t i = 0; i < new_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "new block. blockid: %u\n", new_block_ids->at(i));
          uint32_t physical_block_id = 0;
          BlockFileManager::get_instance()->new_block(new_block_ids->at(i), physical_block_id);
        }
      }

      return EXIT_SUCCESS;
    }

    // gc expired and no referenced datafile
    int DataManagement::gc_data_file()
    {
      int32_t current_time = time(NULL);
      int32_t diff_time = current_time - SYSPARAM_DATASERVER.expire_data_file_time_;

      if (last_gc_data_file_time_ < diff_time)
      {
        data_file_mutex_.lock();
        int32_t old_data_file_size = data_file_map_.size();
        for (DataFileMapIter it = data_file_map_.begin(); it != data_file_map_.end();)
        {
          // no reference and expire
          if (it->second && it->second->get_ref() <= 0 && it->second->get_last_update() < diff_time)
          {
            tbsys::gDelete(it->second);
            data_file_map_.erase(it++);
          }
          else
          {
            ++it;
          }
        }

        int32_t new_data_file_size = data_file_map_.size();

        last_gc_data_file_time_ = current_time;
        data_file_mutex_.unlock();
        TBSYS_LOG(INFO, "datafilemap size. old: %d, new: %d", old_data_file_size, new_data_file_size);
      }

      return TFS_SUCCESS;
    }

    // remove all datafile
    int DataManagement::remove_data_file()
    {
      data_file_mutex_.lock();
      for (DataFileMapIter it = data_file_map_.begin(); it != data_file_map_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
      data_file_map_.clear();
      data_file_mutex_.unlock();
      return TFS_SUCCESS;
    }

  }
}
