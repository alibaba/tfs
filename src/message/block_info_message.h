/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_info_message.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_GETBLOCKINFOMESSAGE_H_
#define TFS_MESSAGE_GETBLOCKINFOMESSAGE_H_
#include "common/base_packet.h"
#include "base_task_message.h"
#include "common/error_msg.h"
namespace tfs
{
  namespace message
  {
#pragma pack(4)
    struct SdbmStat
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int32_t startup_time_;
      int32_t fetch_count_;
      int32_t miss_fetch_count_;
      int32_t store_count_;
      int32_t delete_count_;
      int32_t overflow_count_;
      int32_t page_count_;
      int32_t item_count_;
    };
#pragma pack()

    // get the block information in the common::DataServerStatInfo
    // input argument: mode, block_id
    class GetBlockInfoMessage:  public common::BasePacket
    {
      public:
        GetBlockInfoMessage(const int32_t mode = common::T_READ);
        virtual ~GetBlockInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void add_fail_server(const uint64_t server_id)
        {
          fail_server_.push_back(server_id);
        }
        inline const common::VUINT64* get_fail_server() const
        {
          return &fail_server_;
        }
        inline int32_t get_mode() const
        {
          return mode_;
        }
      protected:
        uint32_t block_id_;
        int32_t mode_;
        common::VUINT64 fail_server_;
    };

    // set the block information in the common::DataServerStatInfo
    // input argument: block_id, server_count, server_id1, server_id2, ..., filename
    class SetBlockInfoMessage:  public common::BasePacket
    {
      public:
        SetBlockInfoMessage();
        virtual ~SetBlockInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set(const uint32_t block_id, const int32_t version, const uint32_t lease_id);
        inline common::VUINT64& get_block_ds()
        {
          return ds_;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline int32_t get_block_version() const
        {
          return version_;
        }
        inline uint32_t get_lease_id() const
        {
          return lease_id_;
        }
        inline bool has_lease() const
        {
          return (lease_id_ != common::INVALID_LEASE_ID);
        }
      private:
        mutable common::VUINT64 ds_;
        uint32_t block_id_;
        mutable int32_t version_;
        mutable uint32_t lease_id_;
    };

    // batch get the block information in the common::DataServerStatInfo
    // input argument: mode, count, block_id
    class BatchGetBlockInfoMessage:  public common::BasePacket
    {
      public:
        BatchGetBlockInfoMessage(int32_t mode = common::T_READ);
        virtual ~BatchGetBlockInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int32_t get_block_count() const
        {
          return (mode_ & common::T_READ) ? block_ids_.size() : block_count_;
        }
        inline void set_block_count(int32_t count)
        {
          block_count_ = count;
        }
        inline void add_block_id(const uint32_t block_id)
        {
          block_ids_.push_back(block_id);
        }
        inline void set_block_id(const common::VUINT32& block_ids)
        {
          block_ids_ = block_ids;
        }
        inline common::VUINT32& get_block_id()
        {
          return block_ids_;
        }
        inline int32_t get_mode() const
        {
          return mode_;
        }
      protected:
        int32_t mode_;
        int32_t block_count_;
        common::VUINT32 block_ids_;
    };

    // batch set the block information in the common::DataServerStatInfo
    // input argument: block_id, server_count, server_id1, server_id2, ..., filename
    class BatchSetBlockInfoMessage:  public common::BasePacket
    {
      public:
        BatchSetBlockInfoMessage();
        virtual ~BatchSetBlockInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_read_block_ds(const uint32_t block_id, common::VUINT64& ds);
        void set_write_block_ds(const uint32_t block_id, common::VUINT64& ds,
            const int32_t version, const int32_t lease_id);
        inline int32_t get_block_count()
        {
          return block_infos_.size();
        }
        inline std::map<uint32_t, common::BlockInfoSeg>& get_infos()
        {
          return block_infos_;
        }
        inline common::VUINT64* get_block_ds(const uint32_t block_id)
        {
          std::map<uint32_t, common::BlockInfoSeg>::iterator it = block_infos_.find(block_id);
          return it == block_infos_.end() ? NULL : &(it->second.ds_);
        }
        inline int32_t get_block_version(const uint32_t block_id)
        {
          std::map<uint32_t, common::BlockInfoSeg>::iterator it = block_infos_.find(block_id);
          return it == block_infos_.end() ? common::EXIT_INVALID_ARGU : it->second.version_;
        }
        inline uint32_t get_lease_id(uint32_t block_id)
        {
          std::map<uint32_t, common::BlockInfoSeg>::iterator it = block_infos_.find(block_id);
          return it == block_infos_.end() ? common::INVALID_LEASE_ID : it->second.lease_id_;
        }
        inline bool get_has_lease(const uint32_t block_id)
        {
          std::map<uint32_t, common::BlockInfoSeg>::iterator it = block_infos_.find(block_id);
          // false ?
          return it == block_infos_.end() ?  false : it->second.has_lease();
        }
      private:
        std::map<uint32_t, common::BlockInfoSeg> block_infos_;
    };

    // block_count, block_id, .....
    class CarryBlockMessage:  public common::BasePacket
    {
      public:
        CarryBlockMessage();
        virtual ~CarryBlockMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void add_expire_id(const uint32_t block_id);
        void add_new_id(const uint32_t block_id);
        void add_remove_id(const uint32_t block_id);
        inline const common::VUINT32* get_expire_blocks() const
        {
          return &expire_blocks_;
        }
        inline const common::VUINT32* get_remove_blocks() const
        {
          return &remove_blocks_;
        }
        inline void set_new_blocks(const common::VUINT32* new_blocks)
        {
          new_blocks_ = (*new_blocks);
        }
        inline const common::VUINT32* get_new_blocks() const
        {
          return &new_blocks_;
        }
      protected:
        common::VUINT32 expire_blocks_;
        common::VUINT32 new_blocks_;
        common::VUINT32 remove_blocks_;
    };

    class NewBlockMessage:  public common::BasePacket
    {
      public:
        NewBlockMessage();
        virtual ~NewBlockMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void add_new_id(const uint32_t block_id);
        inline const common::VUINT32* get_new_blocks() const
        {
          return &new_blocks_;
        }
        inline void set_new_blocks(const common::VUINT32* new_blocks)
        {
          new_blocks_ = (*new_blocks);
        }
      protected:
        common::VUINT32 new_blocks_;
    };

    class RemoveBlockMessage:  public BaseTaskMessage
    {
     public:
        RemoveBlockMessage();
        virtual ~RemoveBlockMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set(const uint32_t id) {  id_ = id;}
        inline uint32_t get() const { return id_;}
        inline void set_response_flag(common::RemoveBlockResponseFlag flag = common::REMOVE_BLOCK_RESPONSE_FLAG_YES)
        {
          response_flag_ = flag;
        }
        inline int8_t get_response_flag() const
        {
          return response_flag_;
        }
      protected:
        uint32_t id_;
        int8_t response_flag_;
    };

    class RemoveBlockResponseMessage :  public BaseTaskMessage
    {
      public:
        RemoveBlockResponseMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        virtual ~RemoveBlockResponseMessage();
        inline void set(const uint32_t id) { block_id_ = id;}
        inline uint32_t get() const { return block_id_;}
      private:
        uint32_t block_id_;
    };

    class ListBlockMessage:  public common::BasePacket
    {
      public:
        ListBlockMessage();
        virtual ~ListBlockMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_type(const int32_t type)
        {
          type_ = type;
        }
        inline int32_t get_block_type() const
        {
          return type_;
        }
      protected:
        int32_t type_;
    };

    class RespListBlockMessage:  public common::BasePacket
    {
      public:
        RespListBlockMessage();
        virtual ~RespListBlockMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void add_block_id(const uint32_t block_id);
        inline void set_status_type(const int32_t type)
        {
          status_type_ = type;
        }
        inline void set_blocks(const int32_t type, const common::VUINT32* v_blocks)
        {
          if (type & common::LB_BLOCK)
          {
            status_type_ = status_type_ | common::LB_BLOCK;
            blocks_ = (*v_blocks);
          }
        }
        inline const common::VUINT32* get_blocks() const
        {
          return &blocks_;
        }
        inline void set_pairs(const int32_t type, std::map<uint32_t, std::vector<uint32_t> >* pairs_)
        {
          if (type & common::LB_PAIRS)
          {
            status_type_ = status_type_ | common::LB_PAIRS;
            block_pairs_ = (*pairs_);
          }
        }
        inline const std::map<uint32_t, std::vector<uint32_t> >* get_pairs() const
        {
          return &block_pairs_;
        }
        inline void set_infos(const int32_t type, const std::map<uint32_t, common::BlockInfo*>* v_infos)
        {
          if (type & common::LB_INFOS)
          {
            status_type_ = status_type_ | common::LB_INFOS;
            block_infos_.clear();
            std::map<uint32_t, common::BlockInfo*>::const_iterator it;
            for (it = v_infos->begin(); it != v_infos->end(); it++)
            {
              block_infos_[it->first] = *(it->second);
            }
          }
        }
        inline const std::map<uint32_t, common::BlockInfo>* get_infos() const
        {
          return &block_infos_;
        }
      protected:
        int32_t status_type_;
        int32_t need_free_;
        common::VUINT32 blocks_;
        std::map<uint32_t, std::vector<uint32_t> > block_pairs_;
        std::map<uint32_t, common::BlockInfo> block_infos_;
    };

    // block_count, block_id, .....
    class UpdateBlockInfoMessage:  public common::BasePacket
    {
      public:
        UpdateBlockInfoMessage();
        virtual ~UpdateBlockInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void set_block(const common::BlockInfo* const block_info)
        {
          if (NULL != block_info)
            block_info_ = *block_info;
        }
        inline const common::BlockInfo* get_block() const
        {
          return &block_info_;
        }
        inline void set_server_id(const uint64_t block_id)
        {
          server_id_ = block_id;
        }
        inline uint64_t get_server_id() const
        {
          return server_id_;
        }
        inline void set_repair(const int32_t repair)
        {
          repair_ = repair;
        }
        inline int32_t get_repair() const
        {
          return repair_;
        }
        inline void set_db_stat(const SdbmStat* const db_stat)
        {
          if (NULL != db_stat)
            db_stat_ = *db_stat;
        }
        inline const SdbmStat* get_db_stat() const
        {
          return &db_stat_;
        }
      protected:
        uint32_t block_id_;
        common::BlockInfo block_info_;
        uint64_t server_id_;
        int32_t repair_;
        SdbmStat db_stat_;
    };

    class ResetBlockVersionMessage:  public common::BasePacket
    {
      public:
        ResetBlockVersionMessage();
        virtual ~ResetBlockVersionMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
      protected:
        uint32_t block_id_;
    };

    class BlockFileInfoMessage:  public common::BasePacket
    {
      public:
        BlockFileInfoMessage();
        virtual ~BlockFileInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline common::FILE_INFO_LIST* get_fileinfo_list()
        {
          return &fileinfo_list_;
        }
      protected:
        uint32_t block_id_;
        common::FILE_INFO_LIST fileinfo_list_;
    };

    class BlockRawMetaMessage:  public common::BasePacket
    {
      public:
        BlockRawMetaMessage();
        virtual ~BlockRawMetaMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline common::RawMetaVec* get_raw_meta_list()
        {
          return &raw_meta_list_;
        }
      protected:
        uint32_t block_id_;
        common::RawMetaVec raw_meta_list_;
    };

    class BlockWriteCompleteMessage:  public common::BasePacket
    {
      public:
        BlockWriteCompleteMessage();
        virtual ~BlockWriteCompleteMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block(const common::BlockInfo* const block_info)
        {
          if (NULL != block_info)
            block_info_ = *block_info;
        }
        inline const common::BlockInfo* get_block() const
        {
          return &block_info_;
        }
        inline void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }
        inline uint64_t get_server_id() const
        {
          return server_id_;
        }
        inline void set_lease_id(const int32_t lease_id)
        {
          lease_id_ = lease_id;
        }
        inline int32_t get_lease_id() const
        {
          return lease_id_;
        }
        inline void set_success(const common::WriteCompleteStatus status)
        {
          write_complete_status_ = status;
        }
        inline common::WriteCompleteStatus get_success() const
        {
          return write_complete_status_;
        }
        inline void set_unlink_flag(const common::UnlinkFlag flag)
        {
          unlink_flag_ = flag;
        }
        inline common::UnlinkFlag get_unlink_flag() const
        {
          return unlink_flag_;
        }
      protected:
        common::BlockInfo block_info_;
        uint64_t server_id_;
        int32_t lease_id_;
        common::WriteCompleteStatus write_complete_status_;
        common::UnlinkFlag unlink_flag_;
    };

    class ListBitMapMessage:  public common::BasePacket
    {
      public:
        ListBitMapMessage();
        virtual ~ListBitMapMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_bitmap_type(const int32_t type)
        {
          type_ = type;
        }
        inline int32_t get_bitmap_type()
        {
          return type_;
        }
      protected:
        int32_t type_;
    };

    class RespListBitMapMessage:  public common::BasePacket
    {
      public:
        RespListBitMapMessage();
        virtual ~RespListBitMapMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_length(const int32_t length)
        {
          ubitmap_len_ = length;
        }
        inline int32_t get_length() const
        {
          return ubitmap_len_;
        }
        inline void set_use_count(const int32_t count)
        {
          uuse_len_ = count;
        }
        inline int32_t get_use_count() const
        {
          return uuse_len_;
        }
        char* alloc_data(const int32_t len);
        char* get_data() const
        {
          return data_;
        }
      protected:
        int32_t ubitmap_len_;
        int32_t uuse_len_;
        char* data_;
        bool alloc_;
    };
  }
}

#endif
