/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: root_server_heart_manager.h 590 2011-09-28 10:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_ROOTSERVER_ROOT_SERVER_HEART_MANAGER_H_
#define TFS_ROOTSERVER_ROOT_SERVER_HEART_MANAGER_H_

#include <Time.h>
#include <Mutex.h>
#include <Monitor.h>
#include <TbThread.h>

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

#include "common/lock.h"
#include "common/buffer.h"
#include "common/rts_define.h"

namespace tfs
{
  namespace rootserver
  {
    class MetaServerManager;
    class RootServerHeartManager
    {
      #ifdef TFS_GTEST
      friend class RootServerHeartManagerTest;
      #endif
      //FRIEND_TEST(RootServerHeartManagerTest, exist);
    public:
      explicit RootServerHeartManager(MetaServerManager& manager);
      virtual ~RootServerHeartManager();
      int initialize(void);
      int destroy();
      bool exist(const uint64_t id);
      bool lease_exist(const uint64_t id);
      int keepalive(const int8_t type, common::RootServerInformation& server);
      int dump_rs_server(void);
      int dump_rs_server(common::Buffer& buffer);
    private:
      uint64_t new_lease_id(void);
      int register_(common::RootServerInformation& server);
      int unregister(const uint64_t id);
      int renew(common::RootServerInformation& server);
      void check_rs_lease_expired_helper(const tbutil::Time& now);
      int rs_role_establish_helper(common::RsRuntimeGlobalInformation& rgi, const int64_t now);
      bool check_vip_helper(common::RsRuntimeGlobalInformation& rgi);
      void check(void);

      int keepalive(common::RtsRsKeepAliveType& type, int32_t& wait_time, tbutil::Time& lease_expired,
          common::RsRuntimeGlobalInformation& rgi, const tbutil::Time& now);
      int get_tables_from_rs(const uint64_t server);
    private:
      class CheckThreadHelper: public tbutil::Thread
      {
      public:
        explicit CheckThreadHelper(RootServerHeartManager& manager): manager_(manager){start();}
        virtual ~CheckThreadHelper(){};
        void run();
      private:
        RootServerHeartManager& manager_;
        DISALLOW_COPY_AND_ASSIGN(CheckThreadHelper);
      };
      typedef tbutil::Handle<CheckThreadHelper> CheckThreadHelperPtr;
    private:
      MetaServerManager& manager_;
      static const int8_t MAX_RETRY_COUNT;
      static const int16_t MAX_TIMEOUT_MS;
      common::ROOT_SERVER_MAPS servers_;
      tbutil::Monitor<tbutil::Mutex> monitor_;
      CheckThreadHelperPtr check_thread_;
      volatile uint64_t lease_id_factory_;
      common::RtsRsKeepAliveType keepalive_type_;
      bool initialize_;
      bool destroy_;
    private:
      DISALLOW_COPY_AND_ASSIGN(RootServerHeartManager);
   };
  } /** rootserver **/
}/** tfs **/

#endif
