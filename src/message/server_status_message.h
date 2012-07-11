/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: server_status_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_SERVERSTATUSMESSAGE_H_
#define TFS_MESSAGE_SERVERSTATUSMESSAGE_H_
#include "common/func.h"
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class GetServerStatusMessage: public common::BasePacket 
    {
      public:
        GetServerStatusMessage();
        virtual ~GetServerStatusMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_status_type(const int32_t type)
        {
          status_type_ = type;
        }
        inline int32_t get_status_type() const
        {
          return status_type_;
        }
        inline void set_from_row(const int32_t row)
        {
          from_row_ = row;
        }
        inline int32_t get_from_row() const
        {
          return from_row_;
        }
        inline void set_return_row(const int32_t row)
        {
          return_row_ = row;
        }
        inline int32_t get_return_row() const
        {
          return return_row_;
        }
      protected:
        int32_t status_type_;
        int32_t from_row_;
        int32_t return_row_;
    };

    class AccessStatInfoMessage: public common::BasePacket 
    {
      public:
        typedef __gnu_cxx ::hash_map<uint32_t, common::Throughput> COUNTER_TYPE;

        AccessStatInfoMessage();
        virtual ~AccessStatInfoMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_from_row(int32_t start)
        {
          if (start < 0)
          {
            start = 0;
          }
          from_row_ = start;
        }
        inline int32_t get_from_row() const
        {
          return from_row_;
        }
        inline void set_return_row(int32_t row)
        {
          if (row < 0)
          {
            row = 0;
          }
          return_row_ = row;
        }
        inline int32_t get_return_row() const
        {
          return return_row_;
        }
        inline int32_t has_next() const
        {
          return has_next_;
        }

        inline const COUNTER_TYPE & get() const
        {
          return stats_;
        }

        inline void set(const COUNTER_TYPE & type)
        {
          stats_ = type;
        }

        inline void set(const uint32_t ip, const common::Throughput& through_put)
        {
          stats_.insert(COUNTER_TYPE::value_type(ip, through_put));
        }

      private:
        int set_counter_map(common::Stream& output, const COUNTER_TYPE & map, int32_t from_row, int32_t return_row,
            int32_t size) const;
        int get_counter_map(common::Stream& input, COUNTER_TYPE & map);

      protected:
        COUNTER_TYPE stats_;
        int32_t from_row_;
        int32_t return_row_;
        mutable int32_t has_next_;
    };

    class ShowServerInformationMessage : public common::BasePacket 
    {
    public:
      ShowServerInformationMessage();
      virtual ~ShowServerInformationMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;

      common::SSMScanParameter& get_param()
      {
        return param;
      }
    private:
      common::SSMScanParameter param;
    };
  }
}
#endif
