/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_
#define TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/meta_nameserver_client_message.h"

#include "meta_store_manager.h"
#include "meta_bucket_manager.h"
#include "meta_heart_manager.h"

namespace tfs
{
  namespace namemetaserver
  {
    class MetaServerService : public common::BaseService
    {
    public:
      MetaServerService();
      ~MetaServerService();

    public:
      // override
      virtual tbnet::IPacketStreamer* create_packet_streamer();
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer);
      virtual common::BasePacketFactory* create_packet_factory();
      virtual void destroy_packet_factory(common::BasePacketFactory* factory);
      virtual const char* get_log_file_path();
      virtual const char* get_pid_file_path();
      virtual bool handlePacketQueue(tbnet::Packet* packet, void* args);
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      int do_action(common::BasePacket* packet);
      int do_write(common::BasePacket* packet);
      int do_read(common::BasePacket* packet);
      int do_ls(common::BasePacket* packet);
      int do_update_bucket(common::BasePacket* packet);

      int create(const int64_t app_id, const int64_t uid,
                 const char* file_path, const common::FileType type);
      int rm(const int64_t app_id, const int64_t uid,
             const char* file_path, const common::FileType type);
      int mv(const int64_t app_id, const int64_t uid,
             const char* file_path, const char* dest_file_path, const common::FileType type);

      int write(const int64_t app_id, const int64_t uid,
                const char* file_path,
                const common::FragInfo& in_v_frag_info);

      int read(const int64_t app_id, const int64_t uid, const char* file_path,
               const int64_t offset, const int64_t size,
               common::FragInfo& frag_info, bool& still_have);

      int ls(const int64_t app_id, const int64_t uid, const int64_t pid,
          const char* file_path, const common::FileType,
          std::vector<common::MetaInfo>& meta_info, bool& still_have);
      int ls_from_db(const int64_t app_id, const int64_t uid, const int64_t pid,
          const char* file_path, const common::FileType,
          std::vector<common::MetaInfo>& meta_info, bool& still_have);
      int ls_file_from_cache(const int64_t app_id, const int64_t uid, const char* file_path,
          std::vector<MetaInfo>& v_meta_info);

      static int check_frag_info(const common::FragInfo& frag_info);
      static void next_file_name(char* name, int32_t& name_len);
      static void next_file_name_base_on(char* name, int32_t& name_len,
          const char* base_name, const int32_t base_name_len);
      //for test
      CacheRootNode* get_root_node(const int64_t app_id, const int64_t uid)
      {
        return store_manager_.get_root_node(app_id, uid);
      }

    private:
      int get_p_meta_info(CacheRootNode* root_node,
          const std::vector<std::string>& v_name,
          CacheDirMetaNode*& out_p_dir_node,
          CacheDirMetaNode*& out_dir_node);

    private:

      static int parse_name(const char* file_path, std::vector<std::string>& v_name);
      static int32_t get_depth(const std::vector<std::string>& v_name);
      static int get_name(const char* name, char* buffer, const int32_t buffer_len, int32_t& name_len);

      int parse_file_path(const int64_t app_id, const int64_t uid, const char* file_path,
          common::MetaInfo& p_meta_info, char* name, int32_t& name_len);

      int parse_file_path(CacheRootNode* root_node, const char* file_path,
          CacheDirMetaNode*& p_dir_node, int64_t& pp_id, char* name, int32_t& name_len,
          const bool root_ok = false);

      int get_p_meta_info(const int64_t app_id, const int64_t uid,
          const std::vector<std::string>& v_name, common::MetaInfo& out_meta_info);

      int read_frag_info(const std::vector<common::MetaInfo>& v_meta_info,
          const int64_t offset, const int64_t size,
          int32_t& cluster_id, std::vector<common::FragMeta>& v_out_frag_info, bool& still_have);


      void add_new_meta_info(const int64_t pid, const int32_t cluster_id,
          const char* name, const int32_t name_len, const int64_t last_offset,
          std::vector<common::MetaInfo>& tmp_v_meta_info);

      void add_frag_to_meta(std::vector<common::FragMeta>::iterator& frag_meta_begin,
          std::vector<common::FragMeta>::iterator frag_meta_end,
          common::MetaInfo& meta_info, int64_t& last_offset);

      template<class T>
        bool check_not_out_over(const T& v)
        {
          return static_cast<int32_t>(v.size()) < common::MAX_OUT_INFO_COUNT;
        }

      static bool is_sub_dir(const char* sub_dir, const char* parents_dir);

      int check_permission(common::BucketStatus& status, const int64_t pid, const int64_t uid,
          const int64_t version, const uint64_t server) const;
      void dump_stat();

      class GcTimerTask: public tbutil::TimerTask
      {
        public:
          GcTimerTask(MetaServerService& service) : service_(service) {}
          virtual ~GcTimerTask() {}
          virtual void runTimerTask();
        private:
          MetaServerService& service_;
      };
      typedef tbutil::Handle<GcTimerTask> GcTimerTaskPtr;

    private:
      DISALLOW_COPY_AND_ASSIGN(MetaServerService);

      MetaStoreManager store_manager_;
      BucketManager bucket_manager_;
      HeartManager heart_manager_;
      tbutil::Mutex mutex_;
      enum {
        STAT_CREATE_DIR = 0,
        STAT_CREATE_FILE = 1,
        STAT_MV_DIR = 2,
        STAT_MV_FILE = 3,
        STAT_LS_DIR = 4,
        STAT_LS_FILE = 5,
        STAT_READ_FILE = 6,
        STAT_WRITE_FILE = 7,
        STAT_RM_DIR = 8,
        STAT_RM_FILE = 9,
        STAT_SUM = 10,
      };
      uint64_t stat_[STAT_SUM];
    };
  }
}

#endif
