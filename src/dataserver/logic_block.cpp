/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: logic_block.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "logic_block.h"
#include "blockfile_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace std;

    LogicBlock::LogicBlock(const uint32_t logic_block_id, const uint32_t main_blk_key, const std::string& base_path, const time_t now) :
      GCObject(now), logic_block_id_(logic_block_id), avail_data_size_(0), visit_count_(0), last_update_(now),
          last_abnorm_time_(0)
    {
      data_handle_ = new DataHandle(this);
      index_handle_ = new IndexHandle(base_path, main_blk_key);
      physical_block_list_.clear();
    }

    LogicBlock::LogicBlock(const uint32_t logic_block_id, const time_t now) :
      GCObject(now), logic_block_id_(logic_block_id), avail_data_size_(0), visit_count_(0),
          last_update_(now), last_abnorm_time_(0), data_handle_(NULL), index_handle_(NULL)
    {
    }

    LogicBlock::~LogicBlock()
    {
      if (data_handle_)
      {
        delete data_handle_;
        data_handle_ = NULL;
      }

      if (index_handle_)
      {
        delete index_handle_;
        index_handle_ = NULL;
      }
    }

    int LogicBlock::init_block_file(const int32_t bucket_size, const MMapOption mmap_option, const BlockType block_type)
    {
      if (0 == logic_block_id_)
      {
        return EXIT_BLOCKID_ZERO_ERROR;
      }

      DirtyFlag dirty_flag = C_DATA_CLEAN;
      if (C_COMPACT_BLOCK == block_type)
      {
        dirty_flag = C_DATA_COMPACT;
      } else if (C_HALF_BLOCK == block_type)
      {
        dirty_flag = C_DATA_HALF;
      }

      // create index handle
      return index_handle_->create(logic_block_id_, bucket_size, mmap_option, dirty_flag);
    }

    int LogicBlock::load_block_file(const int32_t bucket_size, const MMapOption mmap_option)
    {
      if (0 == logic_block_id_)
      {
        return EXIT_BLOCKID_ZERO_ERROR;
      }

      // startup, mmap index file
      return index_handle_->load(logic_block_id_, bucket_size, mmap_option);
    }

    int LogicBlock::delete_block_file()
    {
      // 1. remove index file
      int ret = index_handle_->remove(logic_block_id_);
      if (TFS_SUCCESS != ret)
        return ret;

      // 2. clear physical block list
      list<PhysicalBlock*>::iterator lit = physical_block_list_.begin();
      for (; lit != physical_block_list_.end(); ++lit)
      {
        (*lit)->clear_block_prefix();
      }
      return TFS_SUCCESS;
    }

    int LogicBlock::rename_index_file()
    {
      return index_handle_->rename(logic_block_id_);
    }

    void LogicBlock::add_physic_block(PhysicalBlock* physic_block)
    {
      physical_block_list_.push_back(physic_block);
      avail_data_size_ += physic_block->get_total_data_len();
      return;
    }

    int LogicBlock::open_write_file(uint64_t& inner_file_id)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);

      //compare the low 32bit. (high 32bit: suffix, low 32bit: seq no).
      return index_handle_->find_avail_key(inner_file_id);
    }

    int LogicBlock::check_block_version(int32_t& remote_version, UpdateBlockType& repair)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = index_handle_->check_block_version(remote_version);
      if (EXIT_BLOCK_DS_VERSION_ERROR == ret)
      {
        repair = UPDATE_BLOCK_REPAIR;
      }
      return ret;
    }

    int LogicBlock::close_write_file(const uint64_t inner_file_id, DataFile* datafile, const uint32_t crc)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);

      RawMeta file_meta;
      // check if exist
      int ret = index_handle_->read_segment_meta(inner_file_id, file_meta);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "file exist, update! blockid: %u, fileid: %" PRI64_PREFIX "u", logic_block_id_, inner_file_id);
      }

      // save backup for roll back if update meta fail
      RawMeta bak_file_meta(file_meta);

      int32_t file_size = datafile->get_length();
      FileInfo tfs_file_info, old_file_info;
      tfs_file_info.id_ = inner_file_id;
      tfs_file_info.size_ = file_size + sizeof(FileInfo);
      tfs_file_info.flag_ = 0;
      tfs_file_info.modify_time_ = time(NULL);
      tfs_file_info.create_time_ = time(NULL);
      tfs_file_info.crc_ = crc;

      bool need_update_meta = true, commit_offset = true;
      // commit
      OperType oper_type = C_OPER_UPDATE;
      int32_t old_size = 0, block_offset = 0;

      // already exist
      if (file_meta.get_file_id() == inner_file_id)
      {
        TBSYS_LOG(INFO, "write file. fileid equal. blockid: %u, fileid: %" PRI64_PREFIX "u", logic_block_id_,
            inner_file_id);

        ret = data_handle_->read_segment_info(&old_file_info, file_meta.get_offset());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "read FileInfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d, offset: %d", logic_block_id_,
              inner_file_id, ret, file_meta.get_offset());
        }
        else // read successful
        {
          // use old time
          tfs_file_info.create_time_ = old_file_info.create_time_;
          // reallocate if require space is larger then origin space
          int32_t require_size = file_size + sizeof(FileInfo);
          if (require_size > old_file_info.usize_)
          {
            old_size = old_file_info.usize_;
            block_offset = index_handle_->get_block_data_offset();

            TBSYS_LOG(
                INFO,
                "update file. require size: %d > origin size: %d. need reallocate, blockid: %u, fileid: %" PRI64_PREFIX "u, offset: %d",
                require_size, old_size, logic_block_id_, inner_file_id, block_offset);

            tfs_file_info.offset_ = block_offset;
            tfs_file_info.usize_ = require_size;
            file_meta.set_offset(block_offset);
          }
          else
          {
            // original space ok, just update, no need to commit block total data offset
            commit_offset = false;
            tfs_file_info.offset_ = file_meta.get_offset();
            tfs_file_info.usize_ = old_file_info.usize_;
          }
          // modify meta size
          file_meta.set_size(require_size);
          index_handle_->update_segment_meta(file_meta.get_key(), file_meta);
        }
      }

      // first allocate space or read fail
      if (file_meta.get_file_id() != inner_file_id || ret)
      {
        need_update_meta = false;
        oper_type = C_OPER_INSERT;
        old_size = 0;
        tfs_file_info.usize_ = file_size + sizeof(FileInfo);
        block_offset = index_handle_->get_block_data_offset();
        tfs_file_info.offset_ = block_offset;
        file_meta.set_key(inner_file_id);
        file_meta.set_size(file_size + sizeof(FileInfo));
        file_meta.set_offset(block_offset);

        ret = index_handle_->write_segment_meta(file_meta.get_key(), file_meta);
        if (TFS_SUCCESS != ret)
        {
          return ret;
        }
      }

      char* tmp_data_buffer = NULL;
      int32_t read_len = 0, read_offset = 0;
      int32_t write_len = 0, write_offset = 0;

      // if extend block
      do
      {
        ret = extend_block(file_meta.get_size(), file_meta.get_offset());
        if (TFS_SUCCESS != ret)
          break;

        // write data from datafile to block
        while ((tmp_data_buffer = datafile->get_data(NULL, &read_len, read_offset)) != NULL)
        {
          if (read_len < 0 || (read_len + read_offset) > file_size)
          {
            TBSYS_LOG(ERROR, "getdata fail, blockid: %u, fileid: %" PRI64_PREFIX "u, size: %d, offset: %d, rlen: %d",
                logic_block_id_, inner_file_id, file_size, read_offset, read_len);
            ret = TFS_ERROR;
            break;
          }

          // read finish
          if (0 == read_len)
          {
            break;
          }

          write_len = read_len;
          // first read, write fileinfo
          if (0 == read_offset)
          {
            write_len += sizeof(FileInfo);
          }

          // write length corrupt
          if (write_offset + write_len > file_meta.get_size())
          {
            ret = EXIT_WRITE_OFFSET_ERROR;
            break;
          }

          // write fileinfo
          if (0 == read_offset)
          {
            char* tmp_write_buffer = new char[write_len];
            memcpy(tmp_write_buffer, &tfs_file_info, sizeof(FileInfo));
            memcpy(tmp_write_buffer + sizeof(FileInfo), tmp_data_buffer, read_len);

            ret = data_handle_->write_segment_data(tmp_write_buffer, write_len, file_meta.get_offset() + write_offset);
            delete[] tmp_write_buffer;
          }
          else                  // write real file data
          {
            ret = data_handle_->write_segment_data(tmp_data_buffer, write_len, file_meta.get_offset() + write_offset);
          }

          // check ret(if disk error)
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(
                ERROR,
                "blockid: %u write data error, fileid: %" PRI64_PREFIX "u, size: %d offset: %d, rlen: %d, oldsize: %d, ret: %d",
                logic_block_id_, inner_file_id, file_size, read_offset, read_len, old_size, ret);
            break;
          }

          read_offset += read_len;
          write_offset += write_len;
        }

        if (TFS_SUCCESS != ret)
          break;

        // update index handle statistics
        if (oper_type == C_OPER_INSERT)
        {
          ret = index_handle_->update_block_info(C_OPER_INSERT, file_meta.get_size());
          if (TFS_SUCCESS != ret)
            break;
        }
        else if (oper_type == C_OPER_UPDATE)
        {
          if (0 != old_size)
          {
            ret = index_handle_->update_block_info(C_OPER_DELETE, old_size);
            if (TFS_SUCCESS != ret)
              break;
            ret = index_handle_->update_block_info(C_OPER_INSERT, file_meta.get_size());
            if (TFS_SUCCESS != ret)
              break;
          }
          else
          {
            ret = index_handle_->update_block_info(C_OPER_UPDATE, 0);
            if (TFS_SUCCESS != ret)
              break;
          }
        }
      }
      while (0);

      TBSYS_LOG(DEBUG, "close write file, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
          inner_file_id, ret);
      if (TFS_SUCCESS != ret) // error occur
      {
        if (need_update_meta)
        {
          // rollback
          index_handle_->update_segment_meta(bak_file_meta.get_key(), bak_file_meta);
        }
      }
      else
      {
        if (commit_offset)
        {
          index_handle_->commit_block_data_offset(file_size + sizeof(FileInfo));
        }
      }
      //flush index
      index_handle_->flush();
      return ret;
    }

    int LogicBlock::read_file(const uint64_t inner_file_id, char* buf, int32_t& nbytes, const int32_t offset, const int8_t flag)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      RawMeta file_meta;
      // 1. get file meta info(offset)
      int ret = index_handle_->read_segment_meta(inner_file_id, file_meta);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read file meta fail, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            inner_file_id, ret);
        return ret;
      }

      // truncate to right read length
      if (offset + nbytes > file_meta.get_size())
      {
        nbytes = file_meta.get_size() - offset;
      }
      if (nbytes < 0)
      {
        TBSYS_LOG(ERROR, "blockid: %u, fileid: %" PRI64_PREFIX "u, read offset: %d, file size: %d, ret: %d",
            logic_block_id_, inner_file_id, offset, file_meta.get_size(), EXIT_READ_OFFSET_ERROR);
        return EXIT_READ_OFFSET_ERROR;
      }

      // 2. get file data
      ret = data_handle_->read_segment_data(buf, nbytes, file_meta.get_offset() + offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read data error, fileid: %" PRI64_PREFIX "u, size: %d, offset: %d, ret: %d",
            logic_block_id_, inner_file_id, nbytes, offset, ret);
        return ret;
      }

      TBSYS_LOG(DEBUG, "blockid: %u read data, fileid: %" PRI64_PREFIX "u, read size: %d, offset: %d, ret: %d",
          logic_block_id_, inner_file_id, nbytes, offset, ret);

      // 3. the first fragment, check fileinfo
      if (0 == offset)
      {
        if ((flag & READ_DATA_OPTION_FLAG_FORCE))
        {
          if ((((FileInfo *) buf)->id_ != inner_file_id)
              || (((((FileInfo *) buf)->flag_) & FI_INVALID) != 0))
          {
            TBSYS_LOG(WARN,
                "find FileInfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, id: %" PRI64_PREFIX "u, flag: %d",
                logic_block_id_, inner_file_id, ((FileInfo *) buf)->id_, ((FileInfo *) buf)->flag_);
            return EXIT_FILE_INFO_ERROR;
          }
        }
        else
        {
          if ((((FileInfo *) buf)->id_ != inner_file_id)
              || (((((FileInfo *) buf)->flag_) & (FI_DELETED | FI_INVALID | FI_CONCEAL)) != 0))
          {
            TBSYS_LOG(WARN,
                "find FileInfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, id: %" PRI64_PREFIX "u, flag: %d",
                logic_block_id_, inner_file_id, ((FileInfo *) buf)->id_, ((FileInfo *) buf)->flag_);
            return EXIT_FILE_INFO_ERROR;
          }
        }
     }

      return TFS_SUCCESS;
    }

    int LogicBlock::read_file_info(const uint64_t inner_file_id, FileInfo& finfo)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      RawMeta file_meta;
      // 1. get file meta info
      int ret = index_handle_->read_segment_meta(inner_file_id, file_meta);
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }

      // users probably read after stat, do optimise here
      // we don't care the result here
      data_handle_->fadvise_readahead(file_meta.get_offset(), file_meta.get_size());

      // 2. get fileinfo data
      ret = data_handle_->read_segment_info(&finfo, file_meta.get_offset());
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }
      TBSYS_LOG(
          DEBUG,
          "read file info, fileid: %" PRI64_PREFIX "u, offset: %d, size: %d, usize: %d, mtime: %d, ctime: %d, flag: %d, crc: %u",
          finfo.id_, finfo.offset_, finfo.size_, finfo.usize_, finfo.modify_time_, finfo.create_time_, finfo.flag_,
          finfo.crc_);

      return TFS_SUCCESS;
    }

    int LogicBlock::rename_file(const uint64_t old_inner_file_id, const uint64_t new_inner_file_id)
    {
      // 1. write lock
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      RawMeta file_meta;
      // 2. check if new file id exist
      int ret = index_handle_->read_segment_meta(new_inner_file_id, file_meta);
      if (TFS_SUCCESS == ret)   // exist
      {
        return EXIT_META_UNEXPECT_FOUND_ERROR;
      }

      // 3. read old file meta info
      ret = index_handle_->read_segment_meta(old_inner_file_id, file_meta);
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }

      FileInfo finfo;
      // 4. read old fileinfo
      ret = data_handle_->read_segment_info(&finfo, file_meta.get_offset());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read FileInfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            old_inner_file_id, ret);
        return ret;
      }
      // 5. update old fileinfo
      finfo.modify_time_ = time(NULL);
      finfo.id_ = new_inner_file_id;

      RawMeta old_file_meta(file_meta);
      file_meta.set_file_id(new_inner_file_id);

      // 6. write new fileinfo
      ret = data_handle_->write_segment_info(&finfo, file_meta.get_offset());
      if (TFS_SUCCESS != ret)
        return ret;

      // 7. write new file meta info
      ret = index_handle_->override_segment_meta(file_meta.get_key(), file_meta);
      if (TFS_SUCCESS != ret)
        return ret;

      // 8. delete old file meta info
      ret = index_handle_->delete_segment_meta(old_inner_file_id);
      if (TFS_SUCCESS != ret)
        return ret;

      // 9. flush
      return index_handle_->flush();
    }

    int LogicBlock::unlink_file(const uint64_t inner_file_id, const int32_t action, int64_t& file_size)
    {
      // 1. ...
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      RawMeta file_meta;
      // 2. ...
      int ret = index_handle_->read_segment_meta(inner_file_id, file_meta);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read segment meta fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            inner_file_id, ret);
        return ret;
      }

      FileInfo finfo;
      // 3. ...
      ret = data_handle_->read_segment_info(&finfo, file_meta.get_offset());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read FileInfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            inner_file_id, ret);
        return ret;
      }

      file_size = finfo.size_ - FILEINFO_SIZE;

      int32_t oper_type = 0;
      int tmp_flag = 0;
      int tmp_action = action;

      // ugly impl
      if (action > REVEAL)
      {
        // get the 5-7th bit
        tmp_flag = (action >> 4) & 0x7;
        tmp_action = SYNC;
      }

      // 4. dispatch action
      switch (tmp_action)
      {
      case SYNC:
        if ((finfo.flag_ & FI_DELETED) != (tmp_flag & FI_DELETED))
        {
          if (tmp_flag & FI_DELETED)
          {
            oper_type = C_OPER_DELETE;
          }
          else
          {
            oper_type = C_OPER_UNDELETE;
          }
        }
        finfo.flag_ = tmp_flag;
        break;
      case DELETE:
        if ((finfo.flag_ & (FI_DELETED | FI_INVALID)) != 0)
        {
          TBSYS_LOG(ERROR, "file already deleted, blockid: %u, fileid: %" PRI64_PREFIX "u", logic_block_id_,
              inner_file_id);
          return EXIT_FILE_STATUS_ERROR;
        }

        finfo.flag_ |= FI_DELETED;
        oper_type = C_OPER_DELETE;
        break;
      case UNDELETE:
        if ((finfo.flag_ & FI_DELETED) == 0)
        {
          TBSYS_LOG(ERROR, "file is not deleted, blockid: %u, fileid: %" PRI64_PREFIX "u", logic_block_id_,
              inner_file_id);
          return EXIT_FILE_STATUS_ERROR;
        }

        finfo.flag_ &= (~FI_DELETED);
        oper_type = C_OPER_UNDELETE;
        break;
      case CONCEAL:
        if ((finfo.flag_ & (FI_DELETED | FI_INVALID | FI_CONCEAL)) != 0)
        {
          TBSYS_LOG(ERROR, "file is already deleted or concealed, blockid: %u, fileid: %" PRI64_PREFIX "u",
              logic_block_id_, inner_file_id);
          return EXIT_FILE_STATUS_ERROR;
        }

        finfo.flag_ |= FI_CONCEAL;
        break;
      case REVEAL:
        if ((finfo.flag_ & FI_CONCEAL) == 0 || (finfo.flag_ & (FI_DELETED | FI_INVALID)) != 0)
        {
          TBSYS_LOG(ERROR, "file is not deleted or concealed, blockid: %u, fileid: %" PRI64_PREFIX "u",
              logic_block_id_, inner_file_id);
          return EXIT_FILE_STATUS_ERROR;
        }
        finfo.flag_ &= (~FI_CONCEAL);
        break;
      default:
        TBSYS_LOG(ERROR, "action is illegal. action: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", action,
            logic_block_id_, inner_file_id, EXIT_FILE_ACTION_ERROR);
        return EXIT_FILE_ACTION_ERROR;
      }

      finfo.modify_time_ = time(NULL);

      // 5. write updated fileinfo
      // do not delete index immediately, delete it when compact
      ret = data_handle_->write_segment_info((const FileInfo*) &finfo, file_meta.get_offset());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write segment info fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            inner_file_id, ret);
        return ret;
      }

      // 6. update index meta statistics
      switch (oper_type)
      {
      case C_OPER_DELETE:
        ret = index_handle_->update_block_info(C_OPER_DELETE, file_meta.get_size());
        break;
      case C_OPER_UNDELETE:
        ret = index_handle_->update_block_info(C_OPER_UNDELETE, file_meta.get_size());
        break;
      default:
        break;
      }
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "update block info fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", logic_block_id_,
            inner_file_id, ret);
        return ret;
      }

      // 7. flush
      return index_handle_->flush();
    }

    // just read data, consider no data type
    int LogicBlock::read_raw_data(char* buf, int32_t& nbytes, const int32_t offset)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      if (offset + nbytes > index_handle_->get_block_data_offset())
      {
        nbytes = index_handle_->get_block_data_offset() - offset;
      }
      if (nbytes < 0)
      {
        TBSYS_LOG(ERROR, "blockid: %u, batch read data offset: %d, block current offset: %d",
            logic_block_id_, offset, index_handle_->get_block_data_offset());
        return EXIT_READ_OFFSET_ERROR;
      }

      int ret = data_handle_->read_segment_data(buf, nbytes, offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR,
            "blockid: %u read data batch fail, size: %d, offset: %d, ret: %d",
            logic_block_id_, nbytes, offset, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    void LogicBlock::reset_seq_id(uint64_t file_id)
    {
      index_handle_->reset_avail_key(file_id);
    }

    int LogicBlock::reset_block_version()
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      index_handle_->reset_block_version();
      return index_handle_->flush();
    }

    // just write data, consider no data type
    int LogicBlock::write_raw_data(const char* buf, const int32_t nbytes, const int32_t offset)
    {
      if (NULL == buf)
      {
        return EXIT_POINTER_NULL;
      }

      //limit the nbytes of per write
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = extend_block(nbytes, offset);
      if (TFS_SUCCESS != ret)
        return ret;

      ret = data_handle_->write_segment_data(buf, nbytes, offset);
      if (TFS_SUCCESS != ret)
        return ret;

      // update block total data offset(size)
      index_handle_->commit_block_data_offset(nbytes);
      return index_handle_->flush();
    }

    int LogicBlock::batch_write_meta(const BlockInfo* blk_info, const RawMetaVec* meta_list)
    {
      if (NULL == meta_list)
      {
        return EXIT_POINTER_NULL;
      }
      // 1. ...
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      // 2. update index block info
      ret = copy_block_info(blk_info);
      if (TFS_SUCCESS != ret && EXIT_POINTER_NULL != ret)
      {
        return ret;
      }
      TBSYS_LOG(DEBUG, "batch write meta list, blockid: %u, meta size: %zd", logic_block_id_, meta_list->size());
      // 3. write file meta info
      ret = index_handle_->batch_override_segment_meta(*meta_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "batch override segment meta fail. blockid: %u, ret: %d", logic_block_id_, ret);
        return ret;
      }

      // clear HALF BLOCK flag, it will become a normal block
      ret = set_block_dirty_type(C_DATA_CLEAN);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact blockid: %u set dirty flag fail. ret: %d\n", get_logic_block_id(), ret);
        return ret;
      }

      // 4. flush
      return index_handle_->flush();
    }

    int LogicBlock::copy_block_info(const BlockInfo* blk_info)
    {
      if (NULL == blk_info)
      {
        return EXIT_POINTER_NULL;
      }

      // just copy update
      int ret = index_handle_->copy_block_info(blk_info);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "copy block info fail. blockid: %u, ret: %d", logic_block_id_, ret);
        return ret;
      }
      return TFS_SUCCESS;
    }

    int LogicBlock::get_block_info(BlockInfo* blk_info)
    {
      int ret = TFS_SUCCESS;
      if (NULL == blk_info)
      {
        ret = EXIT_POINTER_NULL;
      }
      else
      {
        BlockInfo* info = index_handle_->block_info();
        if (NULL == info)
        {
          ret = EXIT_POINTER_NULL;
          TBSYS_LOG(ERROR, "get block info fail. blockid: %u, ret: %d", logic_block_id_, ret);
        }
        else
        {
          memcpy(blk_info, info, sizeof(BlockInfo));
        }
      }
      return ret;
    }

    int LogicBlock::set_block_dirty_type(const DirtyFlag dirty_flag)
    {
      index_handle_->set_block_dirty_type(dirty_flag);
      return index_handle_->flush();
    }

    int LogicBlock::get_meta_infos(RawMetaVec& raw_metas)
    {
      raw_metas.clear();
      return index_handle_->traverse_segment_meta(raw_metas);
    }

    int LogicBlock::get_sorted_meta_infos(RawMetaVec& raw_metas)
    {
      raw_metas.clear();
      return index_handle_->traverse_sorted_segment_meta(raw_metas);
    }

    int LogicBlock::get_file_infos(std::vector<FileInfo>& fileinfos)
    {
      fileinfos.clear();
      FileIterator* fit = new FileIterator(this);
      // traverse all file info
      while (fit->has_next())
      {
        int ret = fit->next();
        if (TFS_SUCCESS != ret)
        {
          delete fit;
          return ret;
        }
        const FileInfo* pfi = fit->current_file_info();
        fileinfos.push_back(*pfi);
      }
      delete fit;
      return TFS_SUCCESS;
    }

    int LogicBlock::extend_block(const int32_t size, const int32_t offset)
    {
      int32_t retry_times = MAX_EXTEND_TIMES;
      // extend retry_times extend block in one call
      while (retry_times)
      {
        if (offset + size > avail_data_size_) // need extend block
        {
          TBSYS_LOG(INFO,
              "blockid: %u need ext block. offset: %d, datalen: %d, availsize: %d, data curr offset: %d, retry: %d",
              logic_block_id_, offset, size, avail_data_size_, index_handle_->get_block_data_offset(), retry_times);

          uint32_t physical_ext_blockid = 0;
          uint32_t physical_blockid = 0;
          // get the last prev block id of this logic block
          std::list<PhysicalBlock*>* physcial_blk_list = &physical_block_list_;
          if (0 == physcial_blk_list->size())
          {
            TBSYS_LOG(ERROR, "blockid: %u physical block list is empty!", logic_block_id_);
            return EXIT_PHYSICALBLOCK_NUM_ERROR;
          }

          physical_blockid = physcial_blk_list->back()->get_physic_block_id();
          // new one ext block
          PhysicalBlock* tmp_physic_block = NULL;
          int ret = BlockFileManager::get_instance()->new_ext_block(logic_block_id_, physical_blockid,
              physical_ext_blockid, &tmp_physic_block);
          if (TFS_SUCCESS != ret)
            return ret;

          // update avail size(extend size)
          physical_block_list_.push_back(tmp_physic_block);
          avail_data_size_ += tmp_physic_block->get_total_data_len();
        }
        else                    // no extend block need
        {
          break;
        }
        --retry_times;
      }

      if (0 == retry_times)
      {
        TBSYS_LOG(ERROR, "blockid: %u extend block too much!", logic_block_id_);
        return EXIT_PHYSICALBLOCK_NUM_ERROR;
      }
      return TFS_SUCCESS;
    }

    void LogicBlock::clear()
    {
      // clean logic block associate stuff(index handle, physic block)
      delete_block_file();

      // clean physic block
      for (list<PhysicalBlock*>::iterator lit = physical_block_list_.begin(); lit != physical_block_list_.end(); ++lit)
      {
        tbsys::gDelete(*lit);
      }
    }

    void LogicBlock::callback()
    {
      clear();
    }

    FileIterator::FileIterator(LogicBlock* logic_block)
    {
      logic_block_ = logic_block;
      buf_ = new char[MAX_COMPACT_READ_SIZE];
      data_len_ = 0;
      data_offset_ = 0;
      read_offset_ = -1;
      buf_len_ = 0;
      memset(&cur_fileinfo_, 0, sizeof(FileInfo));
      logic_block_->index_handle_->traverse_sorted_segment_meta(meta_infos_);
      meta_it_ = meta_infos_.begin();
      is_big_file_ = false;
    }

    FileIterator::~FileIterator()
    {
      if (NULL != buf_)
      {
        delete[] buf_;
        buf_ = NULL;
      }
    }

    bool FileIterator::has_next() const
    {
      // not arrive end of block data and file meta infos
      return read_offset_ < logic_block_->index_handle_->data_file_size() && meta_it_ != meta_infos_.end();
    }

    int FileIterator::next()
    {
      if (read_offset_ >= logic_block_->index_handle_->data_file_size() && meta_it_ == meta_infos_.end())
        return TFS_ERROR;

      is_big_file_ = false;

      int ret;
      if (read_offset_ < 0)
      {
        read_offset_ = 0;
        ret = fill_buffer();
        if (TFS_SUCCESS != ret)
          return ret;
      }

      int32_t file_size = meta_it_->get_size();
      int32_t file_offset = meta_it_->get_offset();
      int32_t relative_offset = file_offset - read_offset_;
      if (file_offset >= logic_block_->index_handle_->data_file_size())
      {
        TBSYS_LOG(ERROR, "get next file, blockid: %u, id: %"PRI64_PREFIX"u, file size: %d, file offset: %d, block size: %d, read offset: %d, relative offset: %d",
            logic_block_->logic_block_id_, meta_it_->get_file_id(), file_size,
            file_offset, logic_block_->index_handle_->data_file_size(), read_offset_, relative_offset);
        return EXIT_META_OFFSET_ERROR;
      }

      TBSYS_LOG(DEBUG,
          "get next file, blockid: %u, id: %"PRI64_PREFIX"u, file size: %d, file offset: %d, relative offset: %d, read offset: %d, buf len: %d\n",
          logic_block_->logic_block_id_, meta_it_->get_file_id(), file_size, file_offset, relative_offset, read_offset_, buf_len_);

      if (relative_offset < 0)
      {
        TBSYS_LOG(ERROR,
            "get next file fail, blockid: %u, id: %"PRI64_PREFIX"u, file size: %d, file offset: %d, relative offset: %d, read offset: %d, buf len: %d, total size: %d\n",
            logic_block_->logic_block_id_, meta_it_->get_file_id(), file_size,
            file_offset, relative_offset, read_offset_, buf_len_, logic_block_->index_handle_->data_file_size());
        return EXIT_META_OFFSET_ERROR;
      }

      // case: skip hole
      while (relative_offset > buf_len_)
      {
        TBSYS_LOG(
            WARN,
            "blockid: %u, file offset skip a max compact size. relative offset: %d, file offset: %d, read offset: %d, buf len: %d, total size: %d\n",
            logic_block_->logic_block_id_, relative_offset, file_offset, read_offset_, buf_len_, logic_block_->index_handle_->data_file_size());
        data_len_ = 0;
        read_offset_ += buf_len_;
        ret = fill_buffer();
        if (TFS_SUCCESS != ret)
          return ret;
        relative_offset = file_offset - read_offset_;
      }

      // case: uncomplete file left in buffer
      if (file_size + relative_offset > buf_len_)
      {
        read_offset_ += buf_len_;
        // file size is large the max compact read size
        if (file_size > MAX_COMPACT_READ_SIZE)
        {
          is_big_file_ = true;
          if (relative_offset + static_cast<int32_t> (sizeof(FileInfo)) <= buf_len_)
          {
            memcpy(&cur_fileinfo_, buf_ + relative_offset, sizeof(FileInfo));
          }
          else
          {
            int32_t read_len = sizeof(FileInfo);
            ret = logic_block_->read_raw_data((char*) &cur_fileinfo_, read_len, file_offset);
            if (TFS_SUCCESS != ret)
              return ret;
          }
          cur_fileinfo_.size_ -= sizeof(FileInfo);
          read_offset_ = file_size + file_offset;
          // already get file info, no left data
          data_len_ = 0;
          ++meta_it_;
        }
        else // if two fileinfo at the same offset, recompute data offset and data len
        {
          data_offset_ = relative_offset;
          data_len_ = buf_len_ - relative_offset;
        }
        ret = fill_buffer();
        if (TFS_SUCCESS != ret)
          return ret;
        if (is_big_file_)
          return TFS_SUCCESS;
        relative_offset = file_offset - read_offset_;
      }

      int32_t left_size = buf_len_ - file_size - relative_offset;
      TBSYS_LOG(DEBUG,
          "read one file, blockid: %u, relative offset: %d, file offset: %d, leftsize: %d, filesize: %d\n",
          logic_block_->logic_block_id_, relative_offset, file_offset, left_size, file_size);
      memcpy(&cur_fileinfo_, buf_ + relative_offset, sizeof(FileInfo));
      if (cur_fileinfo_.id_ != meta_it_->get_file_id() || cur_fileinfo_.size_ != file_size)
      {
        TBSYS_LOG(
            ERROR,
            "FileInfo error. blockid: %u, disk file id: %" PRI64_PREFIX "u, size: %d, index file id: %" PRI64_PREFIX "u, size: %d\n",
            logic_block_->logic_block_id_, cur_fileinfo_.id_, cur_fileinfo_.size_, meta_it_->get_file_id(), file_size);
        cur_fileinfo_.flag_ = FI_INVALID;
      }
      else // valid file will update data offset and datalen
      {
        data_offset_ = relative_offset + file_size;
        data_len_ = left_size;
      }
      cur_fileinfo_.size_ -= sizeof(FileInfo);
      ++meta_it_;

      TBSYS_LOG(DEBUG, "read one file end, blockid: %u, read offset: %u, leftlen: %d\n", logic_block_->logic_block_id_,
          read_offset_, data_len_);

      return TFS_SUCCESS;
    }

    const FileInfo* FileIterator::current_file_info() const
    {
      return &cur_fileinfo_;
    }

    bool FileIterator::is_big_file() const
    {
      return is_big_file_;
    }

    int FileIterator::read_buffer(char* buf, int32_t& nbytes)
    {
      if (NULL == buf || nbytes < cur_fileinfo_.size_)
        return TFS_ERROR;

      int32_t relative_offset = cur_fileinfo_.offset_ - read_offset_;
      // cur_fileinfo_.size_ is just only file data size
      memcpy(buf, buf_ + relative_offset + sizeof(FileInfo), cur_fileinfo_.size_);
      nbytes = cur_fileinfo_.size_;
      return TFS_SUCCESS;
    }

    // before the call of fill_buffer, read_offset_ must be set to correct place
    int FileIterator::fill_buffer()
    {
      // previous left data, copy to buffer start to handle
      if (0 != data_len_)
      {
        // avoid overlap
        memmove(buf_, buf_ + data_offset_, data_len_);
      }

      int32_t read_len = MAX_COMPACT_READ_SIZE - data_len_;
      int ret = logic_block_->read_raw_data(buf_ + data_len_, read_len, read_offset_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "batch read data fail, blockid: %u, offset: %d, read len: %d, ret: %d",
            logic_block_->logic_block_id_, read_offset_, read_len, ret);
        return ret;
      }
      buf_len_ = data_len_ + read_len;
      read_offset_ -= data_len_;
      data_len_ = buf_len_;
      data_offset_ = 0;

      return TFS_SUCCESS;
    }

  }
}
