/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.cpp 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "meta_server_service.h"

#include <zlib.h>
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/base_packet.h"
using namespace tfs::common;
using namespace tfs::message;
using namespace std;
namespace
{
  const int TEST_NO_ROOT = 100;
}

namespace tfs
{
  namespace namemetaserver
  {
    MetaServerService::MetaServerService():
      heart_manager_(bucket_manager_,store_manager_)
    {
      memset(stat_, 0, sizeof(stat_));
    }

    MetaServerService::~MetaServerService()
    {
    }

    tbnet::IPacketStreamer* MetaServerService::create_packet_streamer()
    {
      return new common::BasePacketStreamer();
    }

    void MetaServerService::destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
    {
      tbsys::gDelete(streamer);
    }

    common::BasePacketFactory* MetaServerService::create_packet_factory()
    {
      return new message::MessageFactory();
    }

    void MetaServerService::destroy_packet_factory(common::BasePacketFactory* factory)
    {
      tbsys::gDelete(factory);
    }

    const char* MetaServerService::get_log_file_path()
    {
      return NULL;
    }

    const char* MetaServerService::get_pid_file_path()
    {
      return NULL;
    }

    int MetaServerService::initialize(int argc, char* argv[])
    {
      PROFILER_SET_STATUS(0);
      PROFILER_SET_THRESHOLD(10000);
      UNUSED(argc);
      UNUSED(argv);
      int ret = TFS_SUCCESS;
      if ((ret = SYSPARAM_NAMEMETASERVER.initialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SYSPARAM_NAMEMETASERVER::initialize fail. ret: %d", ret);
      }
      else
      {
        bool bret = MemHelper::init(SYSPARAM_NAMEMETASERVER.free_list_count_, SYSPARAM_NAMEMETASERVER.free_list_count_,
              SYSPARAM_NAMEMETASERVER.free_list_count_);
        TBSYS_LOG(INFO, "MemHelper init. r_free_list_count: %d, d_free_list_count: %d, f_free_list_count: %d",
                  SYSPARAM_NAMEMETASERVER.free_list_count_, SYSPARAM_NAMEMETASERVER.free_list_count_, SYSPARAM_NAMEMETASERVER.free_list_count_);
        if (true != bret)
        {
          TBSYS_LOG(ERROR, "init MemHelper error");
          ret = TFS_ERROR;
        }
        else
        {
          ret = store_manager_.init(SYSPARAM_NAMEMETASERVER.max_pool_size_, SYSPARAM_NAMEMETASERVER.max_cache_size_,
                SYSPARAM_NAMEMETASERVER.gc_ratio_, SYSPARAM_NAMEMETASERVER.max_mutex_size_);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "init store_manager error");
          }
          else
          {
            if (TEST_NO_ROOT != argc)
            {
              MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
              rgi.server_.base_info_.id_ = tbsys::CNetUtil::strToAddr(get_ip_addr(), get_port());
              rgi.server_.base_info_.start_time_ = time(NULL);
              ret = heart_manager_.initialize();
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "init heart_manager error");
              }
              else
              {
                GcTimerTaskPtr task = new GcTimerTask(*this);
                int32_t gc_interval = TBSYS_CONFIG.getInt(CONF_SN_NAMEMETASERVER, CONF_GC_INTERVAL, 10);
                get_timer()->scheduleRepeated(task, tbutil::Time::seconds(gc_interval));
              }
            }
          }
        }
      }
      return ret;
    }

    int MetaServerService::destroy_service()
    {
      heart_manager_.destroy();
      store_manager_.destroy();
      return TFS_SUCCESS;
    }

    bool MetaServerService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      int ret = true;
      BasePacket* base_packet = NULL;
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        base_packet = dynamic_cast<BasePacket*>(packet);
        switch (base_packet->getPCode())
        {
        case FILEPATH_ACTION_MESSAGE:
          ret = do_action(base_packet);
          break;
        case WRITE_FILEPATH_MESSAGE:
          ret = do_write(base_packet);
          break;
        case READ_FILEPATH_MESSAGE:
          ret = do_read(base_packet);
          break;
        case LS_FILEPATH_MESSAGE:
          ret = do_ls(base_packet);
          break;
        case REQ_RT_UPDATE_TABLE_MESSAGE:
          ret = do_update_bucket(base_packet);
          break;
        default:
          ret = EXIT_UNKNOWN_MSGTYPE;
          TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
          break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
      }

      // always return true. packet will be freed by caller
      return true;
    }

    // action over file. create, mv, rm
    int MetaServerService::do_action(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do action fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        FilepathActionMessage* req_fa_msg = dynamic_cast<FilepathActionMessage*>(packet);
        int64_t app_id = req_fa_msg->get_app_id();
        int64_t uid = req_fa_msg->get_user_id();
        const char* file_path = req_fa_msg->get_file_path();
        const char* new_file_path = req_fa_msg->get_new_file_path();
        common::MetaActionOp action = req_fa_msg->get_action();
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. app_id: %"PRI64_PREFIX"d, uid: %"
            PRI64_PREFIX"d, file_path: %s, new_file_path: %s, action: %d ret: %d",
            app_id, uid, file_path, new_file_path, action, ret);

        BucketStatus permission = BUCKET_STATUS_NONE;
        MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
        ret = check_permission(permission, app_id, uid,
            req_fa_msg->get_version(), rgi.server_.base_info_.id_);
        if (ret == TFS_SUCCESS)
        {
          ret = permission >= BUCKET_STATUS_RW ? TFS_SUCCESS : EXIT_NOT_PERM_OPER;
          if (ret == TFS_SUCCESS )
          {
            switch (action)
            {
              case CREATE_DIR:
                ret = create(app_id, uid, file_path, DIRECTORY);
                break;
              case CREATE_FILE:
                ret = create(app_id, uid, file_path, NORMAL_FILE);
                break;
              case REMOVE_DIR:
                ret = rm(app_id, uid, file_path, DIRECTORY);
                break;
              case REMOVE_FILE:
                ret = rm(app_id, uid, file_path, NORMAL_FILE);
                break;
              case MOVE_DIR:
                ret = mv(app_id, uid, file_path, new_file_path, DIRECTORY);
                break;
              case MOVE_FILE:
                ret = mv(app_id, uid, file_path, new_file_path, NORMAL_FILE);
                break;
              default:
                TBSYS_LOG(INFO, "unknown action type: %d", action);
                break;
            }
          }
        }
        if (ret != TFS_SUCCESS)
        {
          ret = req_fa_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
        }
        else
        {
          ret = req_fa_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      return ret;
    }

    // get file frag info to read
    int MetaServerService::do_read(common::BasePacket* packet)
    {
      stat_[STAT_READ_FILE]++;
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do read fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReadFilepathMessage* req_rf_msg = dynamic_cast<ReadFilepathMessage*>(packet);
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do read start."
                  "app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, "
                  "file_path: %s, offset :%"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, ret: %d",
                  req_rf_msg->get_app_id(), req_rf_msg->get_user_id(),
                  req_rf_msg->get_file_path(), req_rf_msg->get_offset(), req_rf_msg->get_size(), ret);

        RespReadFilepathMessage* resp_rf_msg = new RespReadFilepathMessage();
        bool still_have;

        BucketStatus permission = BUCKET_STATUS_NONE;
        MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
        ret = check_permission(permission, req_rf_msg->get_app_id(), req_rf_msg->get_user_id(),
              req_rf_msg->get_version(), rgi.server_.base_info_.id_);
        if (ret == TFS_SUCCESS)
        {
          ret = permission >= BUCKET_STATUS_READ_ONLY ? TFS_SUCCESS : EXIT_NOT_PERM_OPER;
          if (ret == TFS_SUCCESS )
          {
            ret = read(req_rf_msg->get_app_id(), req_rf_msg->get_user_id(), req_rf_msg->get_file_path(),
                   req_rf_msg->get_offset(), req_rf_msg->get_size(),
                   resp_rf_msg->get_frag_info(), still_have);
          }
        }

        if (ret != TFS_SUCCESS)
        {
          tbsys::gDelete(resp_rf_msg);
          ret = req_rf_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
        }
        else
        {
          resp_rf_msg->set_still_have(still_have);
          ret = req_rf_msg->reply(resp_rf_msg);
        }
      }
      return ret;
    }

    // write frag info
    int MetaServerService::do_write(common::BasePacket* packet)
    {
      stat_[STAT_WRITE_FILE]++;
      int ret = TFS_SUCCESS;
      PROFILER_START("do_write");
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do write fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        WriteFilepathMessage* req_wf_msg = dynamic_cast<WriteFilepathMessage*>(packet);

        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. "
                  "app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, file_path: %s, meta_size: %zd ret: %d",
                  req_wf_msg->get_app_id(), req_wf_msg->get_user_id(), req_wf_msg->get_file_path(),
                  req_wf_msg->get_frag_info().v_frag_meta_.size(), ret);
        PROFILER_BEGIN("check permission");

        BucketStatus permission = BUCKET_STATUS_NONE;
        MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
        ret = check_permission(permission, req_wf_msg->get_app_id(), req_wf_msg->get_user_id(),
              req_wf_msg->get_version(), rgi.server_.base_info_.id_);
        PROFILER_END();
        if (ret == TFS_SUCCESS)
        {
          ret = permission >= BUCKET_STATUS_RW ? TFS_SUCCESS : EXIT_NOT_PERM_OPER;
          if (ret == TFS_SUCCESS )
          {
            PROFILER_BEGIN("real_write");
            ret = write(req_wf_msg->get_app_id(), req_wf_msg->get_user_id(),
                    req_wf_msg->get_file_path(), req_wf_msg->get_frag_info());
            PROFILER_END();
          }
        }

        if (ret != TFS_SUCCESS)
        {
          ret = req_wf_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
        }
        else
        {
          ret = req_wf_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
    }

    // list file or dir meta info
    int MetaServerService::do_ls(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do ls fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        LsFilepathMessage* req_lf_msg = dynamic_cast<LsFilepathMessage*>(packet);
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do ls start."
                  " app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d,"
                  " pid: %"PRI64_PREFIX"d, file_path: %s, file_type: %d",
                  req_lf_msg->get_app_id(), req_lf_msg->get_user_id(), req_lf_msg->get_pid(),
                  req_lf_msg->get_file_path(), req_lf_msg->get_file_type());

        bool still_have;
        RespLsFilepathMessage* resp_lf_msg = new RespLsFilepathMessage();

        BucketStatus permission = BUCKET_STATUS_NONE;
        MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
        ret = check_permission(permission, req_lf_msg->get_app_id(), req_lf_msg->get_user_id(),
              req_lf_msg->get_version(), rgi.server_.base_info_.id_);
        if (ret == TFS_SUCCESS)
        {
          ret = permission >= BUCKET_STATUS_READ_ONLY ? TFS_SUCCESS : EXIT_NOT_PERM_OPER;
          if (ret == TFS_SUCCESS )
          {
            if (DIRECTORY == static_cast<FileType>(req_lf_msg->get_file_type()))
            {
              stat_[STAT_LS_DIR]++;
            }
            else
            {
              stat_[STAT_LS_FILE]++;
            }
            ret = ls(req_lf_msg->get_app_id(), req_lf_msg->get_user_id(), req_lf_msg->get_pid(),
                 req_lf_msg->get_file_path(), static_cast<FileType>(req_lf_msg->get_file_type()),
                 resp_lf_msg->get_meta_infos(), still_have);
          }
        }

        if (ret != TFS_SUCCESS)
        {
          tbsys::gDelete(resp_lf_msg);
          ret = req_lf_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
        }
        else
        {
          resp_lf_msg->set_still_have(still_have);
          ret = req_lf_msg->reply(resp_lf_msg);
        }
      }
      return ret;
    }

    int MetaServerService::do_update_bucket(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do update bucket fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ret = TFS_ERROR;
        UpdateTableResponseMessage* reply = new UpdateTableResponseMessage();
        UpdateTableMessage* msg = dynamic_cast<UpdateTableMessage*>(packet);
        MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
        if (UPDATE_TABLE_PHASE_1 == msg->get_phase())
        {
          uint64_t dest_length = common::MAX_BUCKET_DATA_LENGTH;
          unsigned char* dest = new unsigned char[common::MAX_BUCKET_DATA_LENGTH];
          ret = uncompress(dest, &dest_length, (unsigned char*)msg->get_table(), msg->get_table_length());
          if (Z_OK != ret)
          {
            TBSYS_LOG(ERROR, "uncompress error: ret : %d, version: %"PRI64_PREFIX"d, dest length: %"PRI64_PREFIX"d, lenght: %"PRI64_PREFIX"d",
              ret, msg->get_version(), dest_length, msg->get_table_length());
            ret = TFS_ERROR;
          }
          else
          {
            ret = bucket_manager_.update_table((const char*)dest, dest_length, msg->get_version(), rgi.server_.base_info_.id_);
          }
          tbsys::gDeleteA(dest);
        }
        else if (UPDATE_TABLE_PHASE_2 == msg->get_phase())
        {
          std::set<int64_t> change;
          ret = bucket_manager_.switch_table(change, rgi.server_.base_info_.id_, msg->get_version());
          if (TFS_SUCCESS == ret)
          {
            //free CacheRootNode, erase from map
            store_manager_.do_lru_gc(change);
          }
        }

        TBSYS_LOG(DEBUG, "MetaNameService::do version: %ld, phase: %d, status: %d",
            msg->get_version(), msg->get_phase(), ret);
        reply->set_version(msg->get_version());
        reply->set_phase(msg->get_phase());
        reply->set_status(ret == TFS_SUCCESS ?  NEW_TABLE_ITEM_UPDATE_STATUS_END : NEW_TABLE_ITEM_UPDATE_STATUS_FAILED);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "MetaNameService::update bucket fail, ret: %d", ret);
        }
        ret = msg->reply(reply);
      }
      return ret;
    }

    int MetaServerService::create(const int64_t app_id, const int64_t uid,
                                  const char* file_path, const FileType type)
    {
      if (DIRECTORY == type)
      {
        stat_[STAT_CREATE_DIR]++;
      }
      else
      {
        stat_[STAT_CREATE_FILE]++;
      }
      PROFILER_START("create");
      int ret = TFS_SUCCESS;
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;

      // not use parse_file_path to avoid dummy reparse.
      std::vector<std::string> v_name;
      CacheDirMetaNode* p_p_dir_node = NULL, *p_dir_node = NULL;
      int64_t pp_id = 0;
      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "file_path(%s) is invalid", file_path);
      }
      if (TFS_SUCCESS == ret && 0 == get_depth(v_name))
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      if (TFS_SUCCESS == ret)
      {
        tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
        tbsys::CThreadGuard mutex_guard(mutex);
        CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);
        if (NULL == root_node)
        {
          TBSYS_LOG(ERROR, "get NULL root node");
          ret = TFS_ERROR;
        }
        else
        {
          if (TFS_SUCCESS == ret)
          {
            if (1 == get_depth(v_name) && NULL == root_node->dir_meta_)
            {
              TBSYS_LOG(DEBUG, "create top dir. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                  app_id, uid, file_path);
              ret = store_manager_.create_top_dir(app_id, uid, root_node);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = get_p_meta_info(root_node, v_name, p_p_dir_node, p_dir_node);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, file_path);
            }
            else
            {
              assert(NULL != p_dir_node);
              if (NULL != p_p_dir_node)
              {
                pp_id = p_p_dir_node->id_;
              }
            }
          }

          if (TFS_SUCCESS == ret)
          {
            PROFILER_BEGIN("insert");
            ret = get_name(v_name[get_depth(v_name)].c_str(), name, MAX_FILE_PATH_LEN, name_len);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "get name fail. ret: %d", ret);
            }
            //void* ret_node = NULL;
            //PROFILER_BEGIN("select1");
            //store_manager_.select(app_id, uid, p_dir_node, name,
            //    false, ret_node);
            //PROFILER_END();
            //if (NULL != ret_node)
            //{
            //  TBSYS_LOG(INFO, "name is a exist dir");
            //  ret = EXIT_TARGET_EXIST_ERROR;
            //}
            //PROFILER_BEGIN("select2");
            //store_manager_.select(app_id, uid, p_dir_node, name,
            //    true, ret_node);
            //PROFILER_END();
            //if (NULL != ret_node)
            //{
            //  TBSYS_LOG(INFO, "name is a exist file");
            //  ret = EXIT_TARGET_EXIST_ERROR;
            //}
            PROFILER_BEGIN("store_insert");
            if (TFS_SUCCESS == ret)
            {
              ret = store_manager_.insert(app_id, uid, pp_id, p_dir_node, name, FileName::length(name), type);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(INFO, "create fail: %s, type: %d, ret: %d", file_path, type, ret);
              }
            }
            PROFILER_END();
            PROFILER_END();
          }

          TBSYS_LOG(DEBUG, "create %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
              TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);
          store_manager_.revert_root_node( root_node);
        }
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
    }

    int MetaServerService::rm(const int64_t app_id, const int64_t uid,
                              const char* file_path, const FileType type)
    {
      if (DIRECTORY == type)
      {
        stat_[STAT_RM_DIR]++;
      }
      else
      {
        stat_[STAT_RM_FILE]++;
      }
      PROFILER_START("rm");
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;

      tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
      tbsys::CThreadGuard mutex_guard(mutex);
      CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);
      CacheDirMetaNode* p_dir_node = NULL;
      int64_t pp_id = 0;
      if (NULL == root_node)
      {
        TBSYS_LOG(ERROR, "get NULL root node");
        ret = TFS_ERROR;
      }
      else
      {
        if ((ret = parse_file_path(root_node, file_path, p_dir_node, pp_id, name, name_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
              app_id, uid, file_path);
        }
        else
        {
          std::vector<MetaInfo> v_meta_info;
          PROFILER_BEGIN("select");

          void* ret_node = NULL;
          ret = store_manager_.select(app_id, uid, p_dir_node, name,
              type != DIRECTORY, ret_node);
          PROFILER_END();

          // file not exist
          if (TFS_SUCCESS != ret || NULL == ret_node)
          {
            ret = EXIT_TARGET_EXIST_ERROR;
          }
          else
          {
            PROFILER_BEGIN("remove");
            if ((ret = store_manager_.remove(app_id, uid, pp_id, p_dir_node, ret_node, type)) != TFS_SUCCESS)
            {
              TBSYS_LOG(DEBUG, "rm fail: %s, type: %d, ret: %d", file_path, type, ret);
            }
            PROFILER_END();
          }
        }

        TBSYS_LOG(DEBUG, "rm %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
            TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);
        store_manager_.revert_root_node( root_node);
      }

      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
    }

    int MetaServerService::mv(const int64_t app_id, const int64_t uid,
                              const char* file_path, const char* dest_file_path,
                              const FileType type)
    {
      if (DIRECTORY == type)
      {
        stat_[STAT_MV_DIR]++;
      }
      else
      {
        stat_[STAT_MV_FILE]++;
      }
      PROFILER_START("mv");
      char name[MAX_FILE_PATH_LEN], dest_name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, dest_name_len = 0;
      int ret = TFS_SUCCESS;
      tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
      tbsys::CThreadGuard mutex_guard(mutex);
      CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);

      CacheDirMetaNode* p_dir_node = NULL;
      CacheDirMetaNode* dest_p_dir_node = NULL;
      int64_t pp_id = 0;
      int64_t dest_pp_id = 0;
      if (NULL == root_node)
      {
        TBSYS_LOG(ERROR, "get NULL root node");
        ret = TFS_ERROR;
      }
      else
      {
        if (DIRECTORY == type)
        {
          if (is_sub_dir(dest_file_path, file_path))
          {
            ret = EXIT_MOVE_TO_SUB_DIR_ERROR;
          }
        }
        if (TFS_SUCCESS == ret)
        {
          if ((ret = parse_file_path(root_node, file_path, p_dir_node, pp_id, name, name_len)) != TFS_SUCCESS)
          {
            TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                app_id, uid, file_path);
          }
          else if ((ret = parse_file_path(root_node, dest_file_path, dest_p_dir_node, dest_pp_id,
                  dest_name, dest_name_len)) != TFS_SUCCESS)
          {
            TBSYS_LOG(INFO, "parse dest file fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                app_id, uid, dest_file_path);
          }
          else
          {
            if ((ret = store_manager_.update(app_id, uid,
                    pp_id, p_dir_node,
                    dest_pp_id, dest_p_dir_node,
                    name, dest_name, type)) != TFS_SUCCESS)
            {
              TBSYS_LOG(DEBUG, "mv fail: %s, type: %d, ret: %d", file_path, type, ret);
            }
          }
        }
        store_manager_.revert_root_node( root_node);

        TBSYS_LOG(DEBUG, "mv %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
            TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);
      }

      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
    }

    int MetaServerService::read(const int64_t app_id, const int64_t uid, const char* file_path,
                                const int64_t offset, const int64_t size,
                                FragInfo& frag_info, bool& still_have)
    {
      PROFILER_START("read");
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;
      still_have = false;

      tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
      tbsys::CThreadGuard mutex_guard(mutex);
      CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);
      CacheDirMetaNode* p_dir_node = NULL;
      int64_t pp_id = 0;
      if (NULL == root_node)
      {
        TBSYS_LOG(ERROR, "get NULL root node");
        ret = TFS_ERROR;
      }
      else
      {
        ret = parse_file_path(root_node, file_path, p_dir_node, pp_id, name, name_len);
        if (TFS_SUCCESS == ret)
        {
          std::vector<MetaInfo> tmp_v_meta_info;
          std::vector<std::string> v_name;
          int64_t last_offset = 0;
          void* ret_file_node = NULL;
          ret = store_manager_.select(app_id, uid, p_dir_node, name, true, ret_file_node);
          if (TFS_SUCCESS == ret)
          {
            if (NULL != ret_file_node)
            {
              CacheFileMetaNode* file_node = (CacheFileMetaNode*)ret_file_node;
              ret = store_manager_.get_file_frag_info(app_id, uid, p_dir_node, file_node,
                  offset, tmp_v_meta_info, frag_info.cluster_id_, last_offset);

              if (TFS_SUCCESS == ret)
              {
                if ((ret = read_frag_info(tmp_v_meta_info, offset, size,
                        frag_info.cluster_id_, frag_info.v_frag_meta_, still_have)) != TFS_SUCCESS)
                {
                  TBSYS_LOG(WARN, "parse read frag info fail. ret: %d", ret);
                }
              }
            }
            else
            {
              ret = EXIT_TARGET_EXIST_ERROR;
            }
          }

        }
        store_manager_.revert_root_node( root_node);
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
    }

    int MetaServerService::write(const int64_t app_id, const int64_t uid,
                                 const char* file_path, const FragInfo& frag_info)
    {
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;
      TBSYS_LOG(DEBUG, "write file_path %s frag_info:", file_path);
      frag_info.dump();

      PROFILER_BEGIN("get mutex");
      tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
      tbsys::CThreadGuard mutex_guard(mutex);
      PROFILER_END();
      PROFILER_BEGIN("get root node");
      CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);
      PROFILER_END();
      CacheDirMetaNode* p_dir_node = NULL;
      int64_t pp_id = 0;
      if (NULL == root_node)
      {
        TBSYS_LOG(ERROR, "get NULL root node");
        ret = TFS_ERROR;
      }
      else
      {
        ret = parse_file_path(root_node, file_path, p_dir_node, pp_id, name, name_len);
        if (TFS_SUCCESS == ret)
        {
          std::vector<MetaInfo> tmp_v_meta_info;
          vector<FragMeta> v_frag_meta(frag_info.v_frag_meta_);
          sort(v_frag_meta.begin(), v_frag_meta.end());
          vector<FragMeta>::iterator write_frag_meta_it = v_frag_meta.begin();

          // we use while, so we can use break instead of goto, no loop here actually
          do
          {
            tmp_v_meta_info.clear();
            int32_t in_cluster_id = -1;
            int64_t last_offset = 0;
            void* ret_file_node = NULL;
            PROFILER_BEGIN("select");
            ret = store_manager_.select(app_id, uid, p_dir_node, name, true, ret_file_node);
            PROFILER_END();
            if (TFS_SUCCESS == ret && NULL == ret_file_node)
            {
              TBSYS_LOG(DEBUG, "file do not exist");
              ret = EXIT_NOT_CREATE_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              CacheFileMetaNode* file_node = (CacheFileMetaNode*)ret_file_node;
              PROFILER_BEGIN("get_file_frag_info");
              ret = store_manager_.get_file_frag_info(app_id, uid, p_dir_node, file_node,
                  write_frag_meta_it->offset_, tmp_v_meta_info, in_cluster_id, last_offset);
              PROFILER_END();
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(DEBUG, "record not exist, name(%s)", name);
                break;
              }
              if (in_cluster_id == -1)
              {
                ret = EXIT_NOT_CREATE_ERROR;
                break;
              }
              if (in_cluster_id != 0 && frag_info.cluster_id_ != in_cluster_id)
              {
                TBSYS_LOG(ERROR, "cluster id error check client code");
                ret = EXIT_CLUSTER_ID_ERROR;
                break;
              }
              if (tmp_v_meta_info.empty())
              {
                //this is for split, we make a new metainfo
                add_new_meta_info(p_dir_node->id_, frag_info.cluster_id_,
                    name, name_len, last_offset, tmp_v_meta_info);
              }

              bool found_meta_info_should_be_updated = false;
              std::vector<MetaInfo>::iterator v_meta_info_it = tmp_v_meta_info.begin();
              TBSYS_LOG(DEBUG, "tmp_v_meta_info.size() = %zd", tmp_v_meta_info.size());
              for (; v_meta_info_it != tmp_v_meta_info.end(); v_meta_info_it++)
              {
                v_meta_info_it->frag_info_.dump();
              }
              v_meta_info_it = tmp_v_meta_info.begin();
              for (; v_meta_info_it != tmp_v_meta_info.end(); v_meta_info_it++)
              {
                if (!v_meta_info_it->frag_info_.had_been_split_ ||//没有分裂
                    (write_frag_meta_it->offset_ != -1 && // offset_ == -1:表示数据追加
                     v_meta_info_it->get_last_offset() > write_frag_meta_it->offset_))
                {
                  found_meta_info_should_be_updated = true;
                  break;
                }
              }

              if (!found_meta_info_should_be_updated)
              {
                TBSYS_LOG(ERROR, "could not found meta info, bug!!");
                ret = EXIT_UPDATE_FRAG_INFO_ERROR;
                break;
              }

              //now write_frag_meta_it  should be write to v_meta_info_it
              if (!v_meta_info_it->frag_info_.had_been_split_)
              {
                add_frag_to_meta(write_frag_meta_it, v_frag_meta.end(), *v_meta_info_it, last_offset);
              }
              else
              {
                int64_t orig_last_offset = v_meta_info_it->get_last_offset();
                while(write_frag_meta_it != v_frag_meta.end())//补全空洞
                {
                  TBSYS_LOG(DEBUG, "write_frag_meta_it->offset_ %"PRI64_PREFIX"d orig_last_offset %"PRI64_PREFIX"d",
                        write_frag_meta_it->offset_, orig_last_offset);
                  if (write_frag_meta_it->offset_ >= orig_last_offset)
                  {
                    ret = EXIT_WRITE_EXIST_POS_ERROR;
                    break;
                  }
                  v_meta_info_it->frag_info_.v_frag_meta_.push_back(*write_frag_meta_it);
                  write_frag_meta_it ++;
                }

                if (TFS_SUCCESS != ret)
                {
                  break;
                }

                // too many frag info
                if (static_cast<int32_t>(v_meta_info_it->frag_info_.v_frag_meta_.size()) >= MAX_FRAG_META_COUNT)
                {
                  ret = EXIT_FRAG_META_OVERFLOW_ERROR;
                  break;
                }
              }

              if (TFS_SUCCESS == ret)
              {
                //resort it
                sort(v_meta_info_it->frag_info_.v_frag_meta_.begin(),
                    v_meta_info_it->frag_info_.v_frag_meta_.end());
                if (v_meta_info_it->frag_info_.cluster_id_ == 0)
                {
                  v_meta_info_it->frag_info_.cluster_id_ = frag_info.cluster_id_;
                }
                ret = check_frag_info(v_meta_info_it->frag_info_);
                if (TFS_SUCCESS == ret)
                {
                  //update this info;
                  v_meta_info_it->file_info_.size_ = v_meta_info_it->get_last_offset();
                  PROFILER_BEGIN("insert frag");
                  ret = store_manager_.insert(app_id, uid, pp_id,
                      p_dir_node, v_meta_info_it->get_name(),
                      v_meta_info_it->get_name_len(), PWRITE_FILE, &(*v_meta_info_it));
                  PROFILER_END();
                }
              }
              assert(write_frag_meta_it == v_frag_meta.end());
            }
          } while (write_frag_meta_it != v_frag_meta.end() && TFS_SUCCESS == ret);

        }
        store_manager_.revert_root_node( root_node);
      }
      return ret;
    }

    int MetaServerService::ls_file_from_cache(const int64_t app_id, const int64_t uid, const char* file_path,
                              std::vector<MetaInfo>& v_meta_info)
    {
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;

      tbsys::CThreadMutex* mutex = store_manager_.get_mutex(app_id, uid);
      tbsys::CThreadGuard mutex_guard(mutex);
      CacheRootNode* root_node = store_manager_.get_root_node(app_id, uid);
      CacheDirMetaNode* p_dir_node = NULL;
      int64_t pp_id = 0;
      if (NULL == root_node)
      {
        TBSYS_LOG(ERROR, "get NULL root node");
        ret = TFS_ERROR;
      }
      else
      {
        if ((ret = parse_file_path(root_node, file_path, p_dir_node, pp_id, name, name_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
              app_id, uid, file_path);
        }
        else
        {

          void* ret_node = NULL;
          ret = store_manager_.select(app_id, uid, p_dir_node, name, true, ret_node);

          // file not exist
          if (TFS_SUCCESS != ret || NULL == ret_node)
          {
            ret = EXIT_TARGET_EXIST_ERROR;
          }
          else
          {
            MetaInfo meta_info;
            CacheFileMetaNode* cache_file_node = (CacheFileMetaNode*)ret_node;
            if (NULL != p_dir_node)
            {
              meta_info.file_info_.pid_ = p_dir_node->id_;
            }
            meta_info.file_info_.create_time_ = cache_file_node->create_time_;
            meta_info.file_info_.modify_time_ = cache_file_node->modify_time_;
            meta_info.file_info_.size_ = cache_file_node->size_;
            v_meta_info.push_back(meta_info);
          }
        }

        store_manager_.revert_root_node( root_node);
      }

      return ret;
    }

    int MetaServerService::ls(const int64_t app_id, const int64_t uid, const int64_t pid,
                              const char* file_path, const common::FileType file_type,
                              std::vector<MetaInfo>& v_meta_info, bool& still_have)
    {
      int ret = 0;
      if (NORMAL_FILE == file_type && -1 == pid) //NORMAL_FILE == file_type and pid != -1 means continue ls dir
      {
        still_have = false;
        ret = ls_file_from_cache(app_id, uid, file_path, v_meta_info);
      }
      else
      {
        ret = ls_from_db(app_id, uid, pid, file_path, file_type, v_meta_info, still_have);
      }
      return ret;
    }

    int MetaServerService::ls_from_db(const int64_t app_id, const int64_t uid, const int64_t pid,
                              const char* file_path, const common::FileType file_type,
                              std::vector<MetaInfo>& v_meta_info, bool& still_have)
    {
      // for inner name data struct
      char name[MAX_FILE_PATH_LEN] = {'\0'};
      int32_t name_len = 1;
      int ret = TFS_SUCCESS;
      FileType my_file_type = file_type;
      bool ls_file = false;
      still_have = true;
      MetaInfo p_meta_info;

      // first ls. parse file info
      if (-1 == pid)
      {
        // just list single file
        ls_file = (file_type != DIRECTORY);

        ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "parse file path fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                    app_id, uid, file_path);
        }
        if (TFS_SUCCESS == ret)
        {
          if (DIRECTORY == file_type) // ls directory, should get current directory info
          {
            if ((ret = store_manager_.get_dir_meta_info(app_id, uid, p_meta_info.get_id(),
                    name, name_len, p_meta_info)) == TFS_SUCCESS)
            {
              // start over
              name[0] = '\0';
              name_len = 1;
            }
          }
        }
      }
      else                        // pid is not -1 means continue last ls
      {
        int32_t file_len = (NULL == file_path ? 0 : strlen(file_path));//file_path：不是全路径
        if (file_len >= MAX_FILE_PATH_LEN)
        {
          TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
          ret = TFS_ERROR;

        }
        else if (file_len > 0)  // continue from file_path
        {
          ret = get_name(file_path, name, MAX_FILE_PATH_LEN, name_len);
          if (TFS_SUCCESS == ret)
          {
            next_file_name(name, name_len);
          }
        }

        p_meta_info.file_info_.id_ = pid;
      }

      if (TFS_SUCCESS == ret)
      {
        MetaInfo last_meta_info;
        std::vector<MetaInfo> tmp_v_meta_info;
        vector<MetaInfo>::iterator tmp_v_meta_info_it;
        do
        {
          tmp_v_meta_info.clear();
          still_have = false;

          // return dir and file separately
          // first dir type, then file type
          ret = store_manager_.ls(app_id, uid, p_meta_info.get_id(),
              name, name_len, NULL, 0,
              my_file_type != DIRECTORY, tmp_v_meta_info, still_have);

          if (!tmp_v_meta_info.empty())
          {
            tmp_v_meta_info_it = tmp_v_meta_info.begin();

            if (my_file_type != DIRECTORY)
            {
              // caclulate file meta info
              store_manager_.calculate_file_meta_info(tmp_v_meta_info_it, tmp_v_meta_info.end(),
                  ls_file, v_meta_info, last_meta_info);
              // ls file only need one meta info
              if (ls_file && !v_meta_info.empty())
              {
                still_have = false;
                break;
              }
            }
            else
            {
              // just push dir's metainfo
              for (; tmp_v_meta_info_it != tmp_v_meta_info.end() && check_not_out_over(v_meta_info);
                  tmp_v_meta_info_it++)
              {
                v_meta_info.push_back(*tmp_v_meta_info_it);
              }
            }

            if (!check_not_out_over(v_meta_info))
            {
              // not list all
              if (tmp_v_meta_info_it != tmp_v_meta_info.end())
              {
                still_have = true;
              }
              break;
            }

            // still have and need continue
            if (still_have)
            {
              tmp_v_meta_info_it--;
              next_file_name_base_on(name, name_len,
                  tmp_v_meta_info_it->get_name(), tmp_v_meta_info_it->get_name_len());
            }
          }

          // dir over, continue list file
          if (my_file_type == DIRECTORY && !still_have)
          {
            my_file_type = NORMAL_FILE;
            still_have = true;
            name[0] = '\0';
            name_len = 1;
          }
        } while (TFS_SUCCESS == ret && still_have);


        if (TFS_SUCCESS == ret && !ls_file && check_not_out_over(v_meta_info)
            && !last_meta_info.empty())
        {
          v_meta_info.push_back(last_meta_info);
        }
      }

      return ret;
    }

    // parse file path.
    // return parent dir's metainfo of current file(dir),
    // and file name(store format) without any prefix dir
    // get info from db, no cache will be affected
     int MetaServerService::parse_file_path(const int64_t app_id, const int64_t uid, const char* file_path,
        MetaInfo& p_meta_info, char* name, int32_t& name_len)
    {
      int ret = TFS_SUCCESS;
      std::vector<std::string> v_name;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "file_path(%s) is invalid", file_path);
      }
      else if (get_depth(v_name) == 0)
      {
        p_meta_info.file_info_.id_ = 0;
        ret = get_name(v_name[0].c_str(), name, MAX_FILE_PATH_LEN, name_len);
      }
      else if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info)) == TFS_SUCCESS)
      {
        ret = get_name(v_name[get_depth(v_name)].c_str(), name, MAX_FILE_PATH_LEN, name_len);
      }

      return ret;
    }
    // parse file path.
    // return parent dir's metainfo of current file(dir),
    // and file name(store format) without any prefix dir
    int MetaServerService::parse_file_path(CacheRootNode* root_node, const char* file_path,
        CacheDirMetaNode*& p_dir_node, int64_t& pp_id,
        char* name, int32_t& name_len, const bool root_ok)
    {
      int ret = TFS_SUCCESS;
      std::vector<std::string> v_name;
      CacheDirMetaNode* pp_dir_node;
      p_dir_node = NULL;
      pp_id = 0;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "file_path(%s) is invalid", file_path);
      }
      else if (get_depth(v_name) == 0)
      {
        if (root_ok) // file_path only has "/"
        {
          ret = get_name(v_name[0].c_str(), name, MAX_FILE_PATH_LEN, name_len);
        }
        else
        {
          ret = TFS_ERROR;
        }
      }
      else if ((ret = get_p_meta_info(root_node, v_name, pp_dir_node, p_dir_node)) == TFS_SUCCESS)
      {
        if (NULL != pp_dir_node)
        {
          pp_id = pp_dir_node->id_;
        }
        if (NULL == p_dir_node)
        {
          //we can not find parent info;
          ret = EXIT_PARENT_EXIST_ERROR;
        }
        else
        {
          ret = get_name(v_name[get_depth(v_name)].c_str(), name, MAX_FILE_PATH_LEN, name_len);
        }
      }

      return ret;
    }


    // get dir parent dir's meta info from db
    int MetaServerService::get_p_meta_info(const int64_t app_id, const int64_t uid,
        const std::vector<std::string>& v_name,
        MetaInfo& out_meta_info)
    {
      int ret = EXIT_PARENT_EXIST_ERROR;
      int32_t depth = get_depth(v_name);

      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;
      std::vector<MetaInfo> tmp_v_meta_info;
      int64_t pid = 0;

      for (int32_t i = 0; i < depth; i++)
      {
        if ((ret = get_name(v_name[i].c_str(), name, MAX_FILE_PATH_LEN, name_len)) != TFS_SUCCESS)
        {
          break;
        }

        ret = store_manager_.select(app_id, uid, pid, name, name_len, false, tmp_v_meta_info);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "select name(%s) failed, ret: %d", name, ret);
          break;
        }

        if (tmp_v_meta_info.empty())
        {
          ret = EXIT_PARENT_EXIST_ERROR;
          TBSYS_LOG(DEBUG, "file(%s) not found, ret: %d", name, ret);
          break;
        }

        // parent's id is its child's pid
        pid = tmp_v_meta_info.begin()->get_id();

        if (i == depth - 1)
        {
          // just get metainfo, no matter which record
          out_meta_info = *(tmp_v_meta_info.begin());
          ret = TFS_SUCCESS;
          break;
        }
      }

      return ret;
    }

    //get parent's info return EXIT_PARENT_EXIST_ERROR if we can not find parent
    int MetaServerService::get_p_meta_info(CacheRootNode* root_node,
        const std::vector<std::string>& v_name,
        CacheDirMetaNode*& out_pp_dir_node,
        CacheDirMetaNode*& out_p_dir_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != root_node);
      int32_t depth = get_depth(v_name);

      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0;

      out_pp_dir_node = NULL;
      out_p_dir_node = root_node->dir_meta_;
      if (NULL == out_p_dir_node)
      {
        TBSYS_LOG(INFO, "\"/\" dir not exist");
        ret = EXIT_PARENT_EXIST_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        void* dir_node = NULL;

        for (int32_t i = 1; i < depth; i++)
        {
          if ((ret = get_name(v_name[i].c_str(), name, MAX_FILE_PATH_LEN, name_len)) != TFS_SUCCESS)
          {
            break;
          }

          ret = store_manager_.select(root_node->key_.app_id_, root_node->key_.uid_,
              out_p_dir_node, name, false, dir_node);

          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "select name(%s) failed, ret: %d", name, ret);
            break;
          }

          if (NULL == dir_node)
          {
            ret = EXIT_PARENT_EXIST_ERROR;
            TBSYS_LOG(DEBUG, "file(%s) not found, ret: %d", name, ret);
            break;
          }
          out_pp_dir_node = out_p_dir_node;
          out_p_dir_node = reinterpret_cast<CacheDirMetaNode*>(dir_node);
        }
      }
      return ret;
    }

    void MetaServerService::add_frag_to_meta(vector<FragMeta>::iterator& frag_meta_begin,
        vector<FragMeta>::iterator frag_meta_end,
        MetaInfo& meta_info, int64_t& last_offset)
    {
      while(frag_meta_begin != frag_meta_end)
      {
        TBSYS_LOG(DEBUG, "last offset = %"PRI64_PREFIX"d %"PRI64_PREFIX"d %d",
            last_offset, frag_meta_begin->offset_, frag_meta_begin->size_);
        if (-1 == frag_meta_begin->offset_) // new metainfo
        {
          frag_meta_begin->offset_ = last_offset;
        }
        else
        {
          last_offset = frag_meta_begin->offset_;
        }
        last_offset += frag_meta_begin->size_;
        meta_info.frag_info_.v_frag_meta_.push_back(*frag_meta_begin);
        frag_meta_begin++;
      }

      if (static_cast<int32_t>(meta_info.frag_info_.v_frag_meta_.size())
          > SOFT_MAX_FRAG_META_COUNT)
      {
        TBSYS_LOG(DEBUG, "split meta_info");
        meta_info.frag_info_.had_been_split_ = true;
      }
    }

    void MetaServerService::add_new_meta_info(const int64_t pid, const int32_t cluster_id,
        const char* name, const int32_t name_len, const int64_t last_offset,
        std::vector<MetaInfo>& tmp_v_meta_info)
    {
      TBSYS_LOG(DEBUG, "add_new_meta_info last_offset = %"PRI64_PREFIX"d", last_offset);
      MetaInfo tmp;
      tmp.file_info_.pid_ = pid;
      tmp.frag_info_.cluster_id_ = cluster_id;
      tmp.frag_info_.had_been_split_ = false;
      char tmp_name[MAX_FILE_PATH_LEN];
      memcpy(tmp_name, name, name_len);
      Serialization::int64_to_char(tmp_name + name_len, MAX_FILE_PATH_LEN - name_len,
          last_offset);
      tmp.file_info_.name_.assign(tmp_name, name_len + 8);
      tmp_v_meta_info.push_back(tmp);
    }

    int MetaServerService::read_frag_info(const vector<MetaInfo>& v_meta_info, const int64_t offset,
        const int64_t size, int32_t& cluster_id,
        vector<FragMeta>& v_out_frag_meta, bool& still_have)
    {
      int64_t end_offset = offset + size;
      vector<MetaInfo>::const_iterator meta_info_it = v_meta_info.begin();;
      cluster_id = -1;
      v_out_frag_meta.clear();
      still_have = false;

      while (meta_info_it != v_meta_info.end())
      {
        cluster_id = meta_info_it->frag_info_.cluster_id_;
        if (meta_info_it->get_last_offset() <= offset)
        {
          meta_info_it++;
          continue;
        }

        const vector<FragMeta>& v_in_frag_meta = meta_info_it->frag_info_.v_frag_meta_;
        if (v_in_frag_meta.empty())
        {
          //no frag to be read
          break;
        }

        FragMeta fragmeta_for_search;
        fragmeta_for_search.offset_ = offset;
        vector<FragMeta>::const_iterator it =
          lower_bound(v_in_frag_meta.begin(), v_in_frag_meta.end(), fragmeta_for_search);

        // check whether previous metainfo is needed
        if (it != v_in_frag_meta.begin())
        {
          vector<FragMeta>::const_iterator tmp_it = it - 1;
          if (offset < tmp_it->offset_ + tmp_it->size_)
          {
            it--;
          }
        }

        still_have = true;

        for(; it != v_in_frag_meta.end() && check_not_out_over(v_out_frag_meta); it++)
        {
          v_out_frag_meta.push_back(*it);
          if (it->offset_ + it->size_ > end_offset)
          {
            still_have = false;
            break;
          }
        }

        if (!meta_info_it->frag_info_.had_been_split_ && it == v_in_frag_meta.end())
        {
          still_have = false;
        }
        break;
      }
      return TFS_SUCCESS;
    }

    int MetaServerService::parse_name(const char* file_path, std::vector<std::string>& v_name)
    {
      int ret = EXIT_INVALID_FILE_NAME;

      if ((file_path[0] != '/') || (strlen(file_path) > (MAX_FILE_PATH_LEN -1)))
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid, length(%zd)", file_path, strlen(file_path));
      }
      else
      {
        v_name.clear();
        v_name.push_back("/");
        Func::split_string(file_path, '/', v_name);
        if (static_cast<int32_t>(v_name.size()) >= SYSPARAM_NAMEMETASERVER.max_sub_dirs_deep_)
        {
          ret = EXIT_OVER_MAX_SUB_DIRS_DEEP;
        }
        else
        {
          ret = TFS_SUCCESS;
        }
      }
      return ret;
    }

    int32_t MetaServerService::get_depth(const std::vector<std::string>& v_name)
    {
      return static_cast<int32_t>(v_name.size() - 1);
    }

    // add length to file name, fit to data struct
    int MetaServerService::get_name(const char* name, char* buffer, const int32_t buffer_len, int32_t& name_len)
    {
      int ret = TFS_SUCCESS;
      //name will be like this
      //    |-----len str----|--8 byte--|
      //[len]xxxxxxxxxxxxxxxx[off_set]
      name_len = strlen(name) + 1;

      if (name_len + 8 > MAX_FILE_PATH_LEN || name_len + 8 >= buffer_len || buffer == NULL)
      {
        TBSYS_LOG(ERROR, "buffer is not enough or buffer is null, %d < %d", buffer_len, name_len);
        ret = TFS_ERROR;
      }
      else
      {
        snprintf(buffer, name_len + 1, "%c%s", char(name_len - 1), name);
        TBSYS_LOG(DEBUG, "old_name: %s, new_name: %.*s, name_len: %d", name, name_len, buffer + 1, name_len);
      }

      return ret;
    }

    int MetaServerService::check_frag_info(const FragInfo& frag_info)
    {
      // TODO. no need check all over..
      int ret = TFS_SUCCESS;

      if (frag_info.v_frag_meta_.size() > 0 && frag_info.cluster_id_ <= 0)
      {
        TBSYS_LOG(ERROR, "cluster id error %d", frag_info.cluster_id_);
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        if (static_cast<int32_t>(frag_info.v_frag_meta_.size())
            > SOFT_MAX_FRAG_META_COUNT &&
            !frag_info.had_been_split_)
        {
          TBSYS_LOG(ERROR, "frag_meta count %zd, should be split",
              frag_info.v_frag_meta_.size());
          ret = TFS_ERROR;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t last_offset = -1;
        for (size_t i = 0; i < frag_info.v_frag_meta_.size(); i++)
        {
          TBSYS_LOG(DEBUG, "frag info %zd off_set %"PRI64_PREFIX"d size %d",
                i, frag_info.v_frag_meta_[i].offset_, frag_info.v_frag_meta_[i].size_);
          if (frag_info.v_frag_meta_[i].offset_ < last_offset)
          {
            TBSYS_LOG(WARN, "frag info have some error, %"PRI64_PREFIX"d < %"PRI64_PREFIX"d",
                frag_info.v_frag_meta_[i].offset_, last_offset);
            ret = EXIT_WRITE_EXIST_POS_ERROR;
            break;
          }

          last_offset = frag_info.v_frag_meta_[i].offset_ +
            frag_info.v_frag_meta_[i].size_;
        }

      }
      return ret;
    }



    void MetaServerService::next_file_name_base_on(char* name, int32_t& name_len,
        const char* base_name, const int32_t base_name_len)
    {
      memcpy(name, base_name, base_name_len);
      name_len = base_name_len;
      next_file_name(name, name_len);
    }

    void MetaServerService::next_file_name(char* name, int32_t& name_len)
    {
      int64_t skip = 1;
      if (name_len == FileName::length(name))
      {
        Serialization::int64_to_char(name + name_len, 8, skip);
        name_len += 8;
      }
      else
      {
        Serialization::char_to_int64(name + name_len - 8, 8, skip);
        skip += 1;
        Serialization::int64_to_char(name + name_len - 8, 8, skip);
      }
    }

    bool MetaServerService::is_sub_dir(const char* sub_dir, const char* parents_dir)
    {
      bool ret = false;
      if (NULL != sub_dir && NULL != parents_dir)
      {
        char* pos = strstr(sub_dir, parents_dir);
        if (pos == sub_dir)
        {
          size_t p_len = strlen(parents_dir);
          size_t c_len = strlen(sub_dir);
          if (p_len == c_len)
          {
            //sub_dir == parents_dir;
            ret = false;
          }
          else
          {
            if ('/' == *(parents_dir + p_len - 1))
            {
              // parents ended with '/'
              ret = true;
            }
            else
            {
              pos += p_len;
              if ('/' == *pos)
              {
                ret = true;
              }
            }
          }
        }
      }
      return ret;
    }

    int MetaServerService::check_permission(BucketStatus& status, const int64_t app_id,
              const int64_t uid, const int64_t version, const uint64_t server) const
    {
      status = BUCKET_STATUS_NONE;
      int32_t iret = app_id > 0 && uid > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = heart_manager_.has_valid_lease() ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
        if (TFS_SUCCESS == iret)
        {
          int64_t bucket_size = bucket_manager_.get_table_size();
          iret = bucket_size > 0 && bucket_size <= common::MAX_BUCKET_ITEM_DEFAULT ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            HashHelper helper(app_id, uid);
            int32_t bucket_id  = tbsys::CStringUtil::murMurHash((const char*)&helper, sizeof(HashHelper)) % bucket_size;
            iret = bucket_manager_.query(status, bucket_id, version, server);
          }
        }
      }
      return iret;
    }
    void MetaServerService::dump_stat()
    {
      TBSYS_LOG(INFO, "cache hit ratio %f", store_manager_.get_cache_hit_ratio());
      TBSYS_LOG(INFO, "CREATE_DIR %"PRI64_PREFIX"d CREATE_FILE %"PRI64_PREFIX"d "
          "MV_DIR %"PRI64_PREFIX"d MV_FILE %"PRI64_PREFIX"d "
          "LS_DIR %"PRI64_PREFIX"d LS_FILE %"PRI64_PREFIX"d "
          "READ %"PRI64_PREFIX"d WRITE %"PRI64_PREFIX"d "
          "RM_DIR %"PRI64_PREFIX"d RM_FILE %"PRI64_PREFIX"d ",
          stat_[STAT_CREATE_DIR], stat_[STAT_CREATE_FILE],
          stat_[STAT_MV_DIR], stat_[STAT_MV_FILE],
          stat_[STAT_LS_DIR], stat_[STAT_LS_FILE],
          stat_[STAT_READ_FILE], stat_[STAT_WRITE_FILE],
          stat_[STAT_RM_DIR], stat_[STAT_RM_FILE]);

          memset(stat_, 0, sizeof(stat_));
          /*stat_[STAT_CREATE_DIR] = 0;
          stat_[STAT_CREATE_FILE] = 0;
          stat_[STAT_MV_DIR] = 0;
          stat_[STAT_MV_FILE] = 0;
          stat_[STAT_LS_DIR] = 0;
          stat_[STAT_LS_FILE] = 0;
          stat_[STAT_READ_FILE] = 0;
          stat_[STAT_WRITE_FILE] = 0;
          stat_[STAT_RM_DIR] = 0;
          stat_[STAT_RM_FILE] = 0;*/
    }
    void MetaServerService::GcTimerTask::runTimerTask()
    {
      service_.store_manager_.do_lru_gc(common::SYSPARAM_NAMEMETASERVER.gc_ratio_);
      service_.dump_stat();
    }
  }/** namemetaserver **/
}/** tfs **/
