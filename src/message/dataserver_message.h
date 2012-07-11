/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_message.h 706 2011-08-12 08:24:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_DATASERVERMESSAGE_H_
#define TFS_MESSAGE_DATASERVERMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class SetDataserverMessage: public common::BasePacket
    {
      public:
        SetDataserverMessage();
        virtual ~SetDataserverMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_ds(common::DataServerStatInfo* ds);
        inline void set_has_block(const common::HasBlockFlag has_block)
        {
          has_block_ = has_block;
        }
        void add_block(common::BlockInfo* block_info);
        inline common::HasBlockFlag get_has_block() const
        {
          return has_block_;
        }
        inline const common::DataServerStatInfo& get_ds() const
        {
          return ds_;
        }
        inline common::BLOCK_INFO_LIST& get_blocks()
        {
          return blocks_;
        }
      protected:
        common::DataServerStatInfo ds_;
        common::BLOCK_INFO_LIST blocks_;
        common::HasBlockFlag has_block_;
        int8_t heart_interval_;
    };

    class CallDsReportBlockRequestMessage: public common::BasePacket
    {
      public:
        CallDsReportBlockRequestMessage();
        virtual ~CallDsReportBlockRequestMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline uint64_t get_server() const
        {
          return server_;
        }
        inline void set_server(const uint64_t server)
        {
          server_ = server;
        }
      private:
        uint64_t server_;
    };

    class ReportBlocksToNsRequestMessage: public common::BasePacket
    {
      public:
        ReportBlocksToNsRequestMessage();
        virtual ~ReportBlocksToNsRequestMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_server(const uint64_t server)
        {
          server_ = server;
        }
        inline uint64_t get_server(void) const
        {
          return server_;
        }
        inline std::set<common::BlockInfo>& get_blocks()
        {
          return blocks_;
        }
      protected:
        std::set<common::BlockInfo> blocks_;
        uint64_t server_;
    };

    class ReportBlocksToNsResponseMessage: public common::BasePacket
    {
      public:
        ReportBlocksToNsResponseMessage();
        virtual ~ReportBlocksToNsResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline std::vector<uint32_t>& get_blocks()
        {
          return expire_blocks_;
        }
        inline void set_server(const uint64_t server)
        {
          server_ = server;
        }
        inline uint64_t get_server(void) const
        {
          return server_;
        }
        inline void set_status(const int8_t status)
        {
          status_ = status;
        }
        inline int8_t get_status(void) const
        {
          return status_;
        }
      protected:
        std::vector<uint32_t> expire_blocks_;
        uint64_t server_;
        int8_t status_;
    };
  }/** end namespace message **/
}/** end namespace tfs **/
#endif
