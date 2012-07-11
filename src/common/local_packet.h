/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_LOCAL_PACKET_H_
#define TFS_COMMON_LOCAL_PACKET_H_
#include "base_packet.h"
#include "new_client.h"
#include "client_manager.h"
namespace tfs
{
  namespace common 
  {
    class LocalPacket: public BasePacket
    {
    public:
      LocalPacket(): new_client_(NULL) { setPCode(LOCAL_PACKET);}
      virtual ~LocalPacket()
      {
        if (NULL != new_client_)
        {
          NewClientManager::free_new_client_object(new_client_);
        }
      }
      bool copy(LocalPacket* , const int32_t , const bool ) { return false;}
      int serialize(Stream& ) const {return TFS_SUCCESS;}
      int deserialize(Stream& ) {return TFS_SUCCESS;}
      int64_t length() const { return 0;}

      inline int execute()
      {
        int32_t iret = NULL != new_client_ ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          new_client_->callback_(new_client_);
        }
        return iret;
      }
      void set_new_client(NewClient* new_client) { new_client_ = new_client;}
      NewClient* get_new_client() const { return new_client_;}
    private:
      DISALLOW_COPY_AND_ASSIGN(LocalPacket);
      NewClient* new_client_;
    };
  }
}
#endif //TFS_COMMON_LOCAL_PACKET_H_
