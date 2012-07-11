/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_backup.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_SYNCBACKUP_H_
#define TFS_DATASERVER_SYNCBACKUP_H_

#include "common/internal.h"
#include "new_client/tfs_client_impl.h"
#include <Memory.hpp>
#include <TbThread.h>

namespace tfs
{
  namespace dataserver
  {
    enum SyncType
    {
      SYNC_TO_TFS_MIRROR = 1,
    };

    struct SyncData
    {
      int32_t cmd_;
      uint32_t block_id_;
      uint64_t file_id_;
      uint64_t old_file_id_;
      int32_t retry_count_;
      int32_t retry_time_;
    };

    class SyncBase;
    class SyncBackup
    {
    public:
      SyncBackup();
      virtual ~SyncBackup();

      virtual bool init() = 0;
      virtual void destroy() = 0;
      virtual int do_sync(const SyncData* sf);
      virtual int copy_file(const uint32_t block_id, const uint64_t file_id);
      virtual int remove_file(const uint32_t block_id, const uint64_t file_id, const int32_t undel);
      virtual int rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id);
      virtual int remote_copy_file(const uint32_t block_id, const uint64_t file_id);

    protected:
      DISALLOW_COPY_AND_ASSIGN(SyncBackup);
      client::TfsClientImpl* tfs_client_;

      char src_addr_[common::MAX_ADDRESS_LENGTH];
      char dest_addr_[common::MAX_ADDRESS_LENGTH];
    };

    class TfsMirrorBackup : public SyncBackup
    {
      public:
        TfsMirrorBackup(SyncBase& sync_base, const char* src_addr, const char* dest_addr);
        virtual ~TfsMirrorBackup();

        bool init();
        void destroy();
        int do_sync(const SyncData* sf);

      private:
        DISALLOW_COPY_AND_ASSIGN(TfsMirrorBackup);

      private:
        int copy_file(const uint32_t block_id, const uint64_t file_id);
        int remove_file(const uint32_t block_id, const uint64_t file_id, const common::TfsUnlinkType action);
        int rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id);
        int remote_copy_file(const uint32_t block_id, const uint64_t file_id);
        int get_file_info(const char* nsip, const char* file_name, common::TfsFileStat& buf);

        /**
        * @brief sync file by stat
        *
        * sync file to same stat in both clusters
        *
        * @param block_id: block id
        * @param file_id:  file id
        *
        * @return
        */
        int sync_stat(const uint32_t block_id, const uint64_t file_id);


        /**
         * @brief if file not exist
         *
         * @param ret: access return value
         *
         * @return
         */
        bool file_not_exist(int ret);

      class DoSyncMirrorThreadHelper: public tbutil::Thread
      {
        public:
          explicit DoSyncMirrorThreadHelper(SyncBase& sync_base):
              sync_base_(sync_base)
          {
            start();
          }
          virtual ~DoSyncMirrorThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(DoSyncMirrorThreadHelper);
          SyncBase& sync_base_;
      };
      typedef tbutil::Handle<DoSyncMirrorThreadHelper> DoSyncMirrorThreadHelperPtr;

    private:
      SyncBase& sync_base_;
      DoSyncMirrorThreadHelperPtr  do_sync_mirror_thread_;

    };

  }
}

#endif //TFS_DATASERVER_SYNCBACKUP_H_
