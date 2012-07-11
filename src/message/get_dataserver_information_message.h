/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: unlink_file_message.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_GET_DATASERVER_INFORMATION_H_
#define TFS_MESSAGE_GET_DATASERVER_INFORMATION_H_

#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class GetDataServerInformationMessage: public common::BasePacket
    {
      public:
        GetDataServerInformationMessage();
        virtual ~GetDataServerInformationMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_flag(const int16_t flag) { flag_ = flag;}
        int16_t get_flag() const { return flag_;}
      private:
        int16_t flag_;
    };

    class GetDataServerInformationResponseMessage: public common::BasePacket
    {
      public:
        GetDataServerInformationResponseMessage();
        virtual ~GetDataServerInformationResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_super_block(const common::SuperBlock& block) { sblock_ = block;}
        void set_dataserver_stat_info(const common::DataServerStatInfo& info) { info_ = info;}
        const common::SuperBlock& get_super_block() const { return sblock_;}
        const common::DataServerStatInfo& get_dataserver_stat_info() const { return info_;}

        int32_t& get_bit_map_element_count() {return  bit_map_element_count_;}

        char* get_data() const { return data_;}

        char* alloc_data(const int64_t length);

      protected:
        common::SuperBlock sblock_;
        common::DataServerStatInfo info_;
        int32_t bit_map_element_count_;
        int32_t data_length_;
        char* data_;
        int16_t flag_;
        bool alloc_;
    };
  }
}
#endif
