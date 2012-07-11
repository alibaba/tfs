/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: table_manager.h 381 2011-09-07 14:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_NAME_META_SERVER_HEART_MANAGER_H_
#define TFS_NAME_META_SERVER_HEART_MANAGER_H_

#include <tbsys.h>
#include <Monitor.h>
#include <Mutex.h>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/rts_define.h"
#include "common/error_msg.h"
#include "meta_store_manager.h"

namespace tfs
{
  namespace namemetaserver
  {
    class BucketManager;
    class HeartManager
    {
    public:
      HeartManager(BucketManager& bucket_manager, MetaStoreManager& store_manager);
      virtual ~HeartManager();

      int initialize(void);
      void destroy();

      inline bool has_valid_lease(void) const { return has_valid_lease_;}

    private:
      void run_heart(void);
      int keepalive(common::RtsMsKeepAliveType& type, int64_t& new_version,
          int32_t& wait_time, tbutil::Time& lease_expired/*, const uint64_t server*/);
      int get_buckets_from_rs(void);
    private:
      class MSHeartBeatThreadHelper: public tbutil::Thread
      {
      public:
        explicit MSHeartBeatThreadHelper(HeartManager& manager): manager_(manager){}
        virtual ~MSHeartBeatThreadHelper(){};
        void run();
      private:
        HeartManager& manager_;
        DISALLOW_COPY_AND_ASSIGN(MSHeartBeatThreadHelper);
      };
      typedef tbutil::Handle<MSHeartBeatThreadHelper> MSHeartBeatThreadHelperPtr;
    private:
      DISALLOW_COPY_AND_ASSIGN(HeartManager);
      static const int8_t MAX_RETRY_COUNT;
      static const int16_t MAX_TIMEOUT_MS;
      BucketManager& bucket_manager_;
      MetaStoreManager& store_manager_;
      MSHeartBeatThreadHelperPtr heart_thread_;
      tbutil::Monitor<tbutil::Mutex> monitor_;
      common::RtsMsKeepAliveType keepalive_type_;
      bool has_valid_lease_;
      bool destroy_;
    };
  }/** namemetaserver **/
}/** tfs **/

#endif

