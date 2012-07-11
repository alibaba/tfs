/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: checkserver.h 746 2012-04-13 13:09:59Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_CHECKSERVER_CHECKSERVER_H
#define TFS_CHECKSERVER_CHECKSERVER_H

#include "tbsys.h"
#include "TbThread.h"
#include "common/internal.h"
#include "common/base_service.h"
#include "server_helper.h"

#include <iostream>
#include <string>
#include <vector>

namespace tfs
{
  namespace checkserver
  {
    class CheckServer;

    /**
     * @brief check dataserver thread
     */
    class CheckThread: public tbutil::Thread
    {
      public:

        explicit CheckThread(CheckBlockInfoMap* result_map, tbutil::Mutex* result_map_lock)
        {
          result_map_ = result_map;
          result_map_lock_ = result_map_lock;
        }

        virtual ~CheckThread()
        {
        }

        /**
         * @brief thread loop
         */
        virtual void run();

        /**
         * @brief add ds to check list
         *
         * @param ds_id: dataserver id
         */
        void add_ds(uint64_t ds_id)
        {
          ds_list_.push_back(ds_id);
        }

        /**
        * @brief set current check time
        *
        * @param check_time
        */
        void set_check_time(const uint32_t check_time)
        {
          check_time_ = check_time;
        }

        /**
        * @brief set last check time
        *
        * @param last_check_time
        */
        void set_last_check_time(const uint32_t last_check_time)
        {
          last_check_time_ = last_check_time;
        }

        /**
         * @brief add check result to map
         *
         * @param result_vec: one ds's result
         */
        void add_result_map(const CheckBlockInfoVec& result_vec);

      private:
        uint32_t check_time_;
        uint32_t last_check_time_;
        VUINT64 ds_list_;                // ds list to check
        CheckBlockInfoMap* result_map_;  // map to store check result
        tbutil::Mutex* result_map_lock_; // lock to modify the map
    };

    typedef tbutil::Handle<CheckThread> CheckThreadPtr;

    /**
     * @brief check dataserver thread
     */
    class RecheckThread: public tbutil::Thread
    {
      public:

        explicit RecheckThread(CheckServer* check_server)
        {
          check_server_ = check_server;
        }

        virtual ~RecheckThread()
        {
        }

        /**
         * @brief thread loop
         */
        virtual void run();

        /**
         * @brief add ds to check list
         *
         * @param ds_id: dataserver id
         */
        void add_block(const uint32_t block_id)
        {
          block_list_.push_back(block_id);
        }

      private:
        CheckServer* check_server_;
        VUINT64 block_list_;          // block list to check
    };

    typedef tbutil::Handle<RecheckThread> RecheckThreadPtr;

    /**
     * @brief check server impl
     */
    class CheckServer: public common::BaseMain
    {
      public:
        CheckServer(): master_ns_id_(0), slave_ns_id_(0), thread_count_(0),
          check_interval_(0), overlap_check_time_(0), block_stable_time_(0),
          last_check_time_(0), index_(-1), mfp_(NULL), sfp_(NULL), meta_fd_(-1)
        {
          factory_ = new MessageFactory();
          streamer_ = new BasePacketStreamer(factory_);
        }

        virtual ~CheckServer()
        {
          if (meta_fd_ >= 0)
          {
            close(meta_fd_);
            meta_fd_ = -1;
          }

          tbsys::gDelete(factory_);
          tbsys::gDelete(streamer_);
        }

        /**
        * @brief help implementation
        */
        virtual void help();

        /**
        * @brief version information
        */
        virtual void version();

        /**
        * @brief destroy method
        *
        * @return
        */
        virtual bool destroy()
        {
          NewClientManager::get_instance().destroy();
          return true;
        }

        /**
        * @brief parse command line
        *
        * @param argc: number of args
        * @param argv[]: arg list
        * @param errmsg: error message
        *
        * @return
        */
        virtual int parse_common_line_args(int argc, char* argv[], std::string& errmsg);

        /**
         * @brief work to do
         *
         * @param argc: number of args
         * @param argv[]: arg list
         *
         * @return
         */
        virtual int run(int argc , char* argv[]);

        /**
        * @brief do initialize work
        */
        int initialize();

        /**
         * @brief main execute line
         */
        int run_check();

        /**
         * @brief check logic cluster
         *
         * @param cluster_info
         *
         * @return
         */
        int check_logic_cluster();

        /**
         * @brief check one physical result
         *
         * @param ns_id: ns address
         * @param cluster_result: result map
         *
         * @return 0 on success
         */
        int check_physical_cluster(const uint64_t ns_id, const uint32_t check_time,
              CheckBlockInfoMap& cluster_result);

        /**
        * @brief recheck different blocks, compact may happen in master
        *
        * @param recheck_block: block list
        *
        * @return
        */
        void recheck_block(const VUINT& recheck_block);

        /**
         * @brief get master ns id
         *
         * @return
         */
        uint64_t get_master_nsid() const
        {
          return master_ns_id_;
        }

        /**
         * @brief get slave ns id
         *
         * @return
         */
        uint64_t get_slave_nsid() const
        {
          return slave_ns_id_;
        }

        void add_m_sync_list(const uint32_t block_id)
        {
          fprintf(mfp_, "%u\n", block_id);
        }

        void add_s_sync_list(const uint32_t block_id)
        {
          fprintf(sfp_, "%u\n", block_id);
        }

        /**
        * @brief decide which block in cluster is main block
        *
        * @param block_infos
        *
        * @return
        */
        CheckBlockInfo& select_main_block(CheckBlockInfoVec& block_infos);

        /**
        * @brief compare block
        *
        * @param left: left object
        * @param right: right object
        *
        */
        void compare_block(const CheckBlockInfo& left, const CheckBlockInfo& right);

      private:
        /**
        * @brief load last check time
        *
        *
        * @return
        */
        int init_meta();

        /**
        * @brief save last check time
        *
        * @return
        */

        void update_meta();
        /**
         * @brief open output file
         *
         * @param check_time
         *
         * @return
         */
        int open_file(const uint32_t check_time);

        /**
         * @brief close output file
         *
         * @return
         */
        void close_file();

        /**
         * @brief diff block between cluster
         *
         * @param master_result: master result
         * @param slave_result: slave result
         * @param recheck_list: blocks need to recheck
         */
        void compare_cluster(CheckBlockInfoMap& master_result,
            CheckBlockInfoMap& slave_result, common::VUINT& recheck_list);

      private:
        uint64_t master_ns_id_;     // first cluster ns id
        uint64_t slave_ns_id_;      // second cluster ns id
        int thread_count_;          // check thread number
        int check_interval_;        // check cluster interval
        int overlap_check_time_;    // overlap time between two checks
        int block_stable_time_;     // block stable time
        uint32_t last_check_time_;  // last check time
        int index_;                 // server index
        char app_name_[MAX_FILE_NAME_LEN];  // app_name

        FILE* mfp_;                 // sync to slave file pointer
        FILE* sfp_;                 // sync to master file pointer
        std::string meta_dir_;      // meta directory
        int32_t meta_fd_;

        MessageFactory *factory_;
        BasePacketStreamer *streamer_;

      private:
        DISALLOW_COPY_AND_ASSIGN(CheckServer);
    };

  }
}

#endif

