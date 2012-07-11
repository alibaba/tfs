/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rollback_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_ROLLBACKMESSAGE_H_
#define TFS_MESSAGE_ROLLBACKMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class RollbackMessage: public common::BasePacket 
    {
    public:
      RollbackMessage();
      virtual ~RollbackMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;
      inline int32_t get_act_type() const
      {
        return act_type_;
      }
      inline void set_act_type(const int32_t type)
      {
        act_type_ = type;
      }
      inline const common::BlockInfo* get_block_info() const
      {
        return &block_info_;
      }
      inline void set_block_info(common::BlockInfo* const block_info)
      {
        if (NULL != block_info)
          block_info_ = *block_info;
      }
      inline const common::FileInfo* get_file_info() const
      {
        return &file_info_;
      }
      inline void set_file_info(common::FileInfo* const file_info) 
      {
        if (NULL != file_info)
          file_info_ = *file_info;
      }
    private:
      int32_t act_type_;
      common::BlockInfo block_info_;
      common::FileInfo file_info_;
    };
  }
}
#endif
