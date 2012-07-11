/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_client_api_impl.cpp 943 2011-10-20 01:40:25Z chuyu $
 *
 * Authors:
 *    daoan<aoan@taobao.com>
 *      - initial release
 *
 */
#include "tfs_meta_client_api_impl.h"

#include <algorithm>
#include <Memory.hpp>
#include "common/func.h"
#include "common/meta_hash_helper.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tfs_client_api.h"
#include "tfs_meta_helper.h"
#include "fsname.h"

namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    void MetaTable::dump()
    {
      TBSYS_LOG(DEBUG, "version_id: %"PRI64_PREFIX"d", version_id_);
      std::vector<uint64_t>::iterator iter = v_meta_table_.begin();
      for(; iter != v_meta_table_.end(); iter++)
      {
        TBSYS_LOG(DEBUG, "meta server addr: %s", tbsys::CNetUtil::addrToString(*iter).c_str());
      }
    }

    NameMetaClientImpl::NameMetaClientImpl()
    {
      packet_factory_ = new message::MessageFactory();
      packet_streamer_ = new common::BasePacketStreamer(packet_factory_);
    }

    NameMetaClientImpl::~NameMetaClientImpl()
    {
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
      tfs_meta_manager_.destroy();
    }

    int NameMetaClientImpl::initialize(const char* rs_addr)
    {
      int ret = TFS_SUCCESS;
      if (rs_addr == NULL)
      {
        TBSYS_LOG(WARN, "rs_addr is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        ret = initialize(Func::get_host_ip(rs_addr));
      }
      return ret;
    }
    int NameMetaClientImpl::initialize(const int64_t rs_addr)
    {
      int ret = TFS_SUCCESS;
      ret = common::NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_);
      if (TFS_SUCCESS == ret)
      {
        rs_id_ = rs_addr;
        update_table_from_rootserver();
        ret = tfs_meta_manager_.initialize();
      }
      return ret;
    }

    int NameMetaClientImpl::create_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      int ret = 0;
      if (NULL == dir_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, CREATE_DIR, dir_path);
      }
      return ret;
    }

    int NameMetaClientImpl::create_dir_with_parents(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      int ret = 0;
      if (NULL == dir_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, CREATE_DIR, dir_path);
        // parent not exist, recursive create it
        if (EXIT_PARENT_EXIST_ERROR == ret)
        {
          char tmp_path[MAX_FILE_PATH_LEN];
          strncpy(tmp_path, dir_path, MAX_FILE_PATH_LEN);
          tmp_path[MAX_FILE_PATH_LEN - 1] = '\0';
          char* parent_dir_path = NULL;
          ret = get_parent_dir(tmp_path, parent_dir_path);
          if (TFS_SUCCESS == ret && NULL != parent_dir_path)
          {
            ret = create_dir_with_parents(app_id, uid, parent_dir_path);
            if (TFS_SUCCESS == ret || EXIT_TARGET_EXIST_ERROR == ret)
            {
              ret = do_file_action(app_id, uid, CREATE_DIR, dir_path);
            }
          }
        }
      }
      return ret;
    }

    int NameMetaClientImpl::create_file(const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      int ret = 0;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, CREATE_FILE, file_path);
      }
      return ret;
    }

    int NameMetaClientImpl::mv_dir(const int64_t app_id, const int64_t uid,
        const char* src_dir_path, const char* dest_dir_path)
    {
      int ret = 0;
      if (NULL == src_dir_path || NULL == dest_dir_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, MOVE_DIR, src_dir_path, dest_dir_path);
      }
      return ret;
    }

    int NameMetaClientImpl::mv_file(const int64_t app_id, const int64_t uid,
        const char* src_file_path, const char* dest_file_path)
    {
      int ret = 0;
      if (NULL == src_file_path || NULL == dest_file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, MOVE_FILE, src_file_path, dest_file_path);
      }
      return ret;
    }

    int NameMetaClientImpl::rm_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      int ret = 0;
      if (NULL == dir_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_file_action(app_id, uid, REMOVE_DIR, dir_path);
      }
      return ret;
    }

    int NameMetaClientImpl::rm_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      FragInfo frag_info;
      int ret = TFS_SUCCESS;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      if (TFS_SUCCESS == ret)
      {
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        if ((ret = read_frag_info(meta_server_id, app_id, uid, file_path, frag_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read frag info error, ret: %d", ret);
        }
        if (TFS_SUCCESS == ret)
        {
          if ((ret = do_file_action(app_id, uid, REMOVE_FILE, file_path)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "remove file failed, file_path: %s, ret: %d", file_path, ret);
          }
          else
          {
            unlink_file(frag_info, ns_addr);
          }
        }
      }
      return ret;
    }

    int NameMetaClientImpl::ls_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path,
        std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive)
    {
      int ret = 0;
      if (NULL == dir_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        v_file_meta_info.clear();
        ret = ls_dir(meta_server_id, app_id, uid, dir_path, -1, v_file_meta_info, is_recursive);
      }
      return ret;
    }

    int NameMetaClientImpl::ls_file(int64_t app_id, int64_t uid,
        const char* file_path, FileMetaInfo& file_meta_info)
    {
      int ret = TFS_SUCCESS;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      if (TFS_SUCCESS == ret)
      {
        vector<FileMetaInfo> v_file_meta_info;
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        TBSYS_LOG(DEBUG, "meta server is %s",tbsys::CNetUtil::addrToString(meta_server_id).c_str());
        if ((ret = do_ls_ex(meta_server_id, app_id, uid, file_path, NORMAL_FILE, -1, v_file_meta_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "ls file failed, file_path: %s, ret: %d", file_path, ret);
        }
        else
        {
          if (v_file_meta_info.size() <= 0)
          {
            TBSYS_LOG(WARN, "ls file returns no file meta info, file_path: %s", file_path);
            ret = EXIT_TARGET_EXIST_ERROR;
          }
          else
          {
            file_meta_info = v_file_meta_info[0];
          }
        }
      }
      return ret;
    }

    bool NameMetaClientImpl::is_dir_exist(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      int bRet = false;
      if (NULL != dir_path)
      {
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        std::vector<common::FileMetaInfo> v_file_meta_info;
        int ret = do_ls_ex(meta_server_id, app_id, uid, dir_path, DIRECTORY, -1, v_file_meta_info, true);

        if(TFS_SUCCESS == ret)
        {
          bRet = true;
        }
        else if (EXIT_TARGET_EXIST_ERROR != ret && EXIT_PARENT_EXIST_ERROR != ret)
        {
          TBSYS_LOG(WARN, "some other error occur, not sure whether dir: %s is exist or not, ret: %d", dir_path, ret);
        }
      }
      return bRet;
    }

    bool NameMetaClientImpl::is_file_exist(const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      int bRet = false;
      if (NULL != file_path)
      {
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        vector<FileMetaInfo> v_file_meta_info;
        int ret = do_ls_ex(meta_server_id, app_id, uid, file_path, NORMAL_FILE, -1, v_file_meta_info, true);
        if (TFS_SUCCESS != ret)
        {
          if (EXIT_TARGET_EXIST_ERROR != ret && EXIT_PARENT_EXIST_ERROR != ret)
          {
            TBSYS_LOG(WARN, "some other error occur, not sure whether file: %s is exist or not, ret: %d", file_path, ret);
          }
        }
        else
        {
          if (v_file_meta_info.size() > 0)
          {
            bRet = true;
          }
        }
      }
      return bRet;
    }

    int64_t NameMetaClientImpl::read(const char* ns_addr, int64_t app_id, int64_t uid,
        const char* file_path, void* buffer, const int64_t offset, const int64_t length)
    {
      int64_t ret = TFS_SUCCESS;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      if (offset < 0 )
      {
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        bool still_have = false;
        int64_t left_length = length;
        int64_t read_length = 0;
        int64_t cur_offset = offset;
        int64_t cur_length = 0;
        int64_t cur_pos = 0;
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);

        do
        {
          FragInfo frag_info;
          int32_t retry = 3;
          int tmp_ret = TFS_SUCCESS;
          do
          {
            if ((tmp_ret = do_read(meta_server_id, app_id, uid, file_path, cur_offset, left_length, frag_info, still_have))
                != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "read file failed, path: %s, ret: %d, retry: %d", file_path, tmp_ret, retry);
            }
          }
          while(tmp_ret == EXIT_NETWORK_ERROR && --retry);
          if (need_update_table(tmp_ret))
          {
            update_table_from_rootserver();
          }
          // file not exist
          if (tmp_ret != TFS_SUCCESS && left_length == length)
          {
            ret = tmp_ret;
            break;
          }
          frag_info.dump();

          if (frag_info.v_frag_meta_.size() <= 0)
          {
            //TBSYS_LOG(ERROR, "get frag info failed");
            //ret = EXIT_TARGET_EXIST_ERROR;
            break;
          }

          cur_length = min(frag_info.get_last_offset() - cur_offset, left_length);

          read_length = read_data(ns_addr, frag_info, reinterpret_cast<char*>(buffer) + cur_pos, cur_offset, cur_length);
          // tfs error occurs, maybe server is down, break and return error
          if (read_length < 0)
          {
            TBSYS_LOG(WARN, "read data from tfs failed, read_length: %"PRI64_PREFIX"d", read_length);
            ret = read_length;
            break;
          }
          // one tfs file read data error, should break
          if (read_length == 0)
          {
            break;
          }

          left_length -= read_length;
          TBSYS_LOG(DEBUG, "@@ read once, offset: %ld, length %ld, read_length: %ld, left: %ld",
              cur_offset, cur_length, read_length, left_length);
          cur_offset += read_length;
          cur_pos += read_length;
        }
        while((left_length > 0) && still_have);
        if (TFS_SUCCESS == ret)
        {
          ret = (length - left_length);
        }
      }
      return ret;
    }

    int64_t NameMetaClientImpl::write(const char* ns_addr, int64_t app_id, int64_t uid,
        const char* file_path, const void* buffer, const int64_t length)
    {
      int64_t ret = 0;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = write(ns_addr, app_id, uid, file_path, buffer, -1, length);
      }
      return ret;
    }

    int64_t NameMetaClientImpl::write(const char* ns_addr, int64_t app_id, int64_t uid,
        const char* file_path, const void* buffer, const int64_t offset, const int64_t length)
    {
      int64_t ret = TFS_SUCCESS;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {

        int32_t cluster_id = -1;
        int64_t left_length = length;
        uint64_t meta_server_id = get_meta_server_id(app_id, uid);
        if (!is_valid_file_path(file_path) || (offset < 0 && offset != -1))
        {
          ret = EXIT_INVALID_ARGU_ERROR;
          TBSYS_LOG(ERROR, "file path is invalid or offset less then 0, offset: %"PRI64_PREFIX"d", offset);
        }
        else if (buffer == NULL || length < 0)
        {
          ret = EXIT_INVALID_ARGU_ERROR;
          TBSYS_LOG(ERROR, "invalid buffer, length %"PRI64_PREFIX"d", length);
        }
        else if((cluster_id = get_cluster_id(meta_server_id, app_id, uid, file_path)) == -1)
        {
          ret = EXIT_INVALID_FILE_NAME;
          TBSYS_LOG(ERROR, "get cluster id error, cluster_id: %d", cluster_id);
        }
        else
        {
          int64_t cur_offset = offset;
          int64_t cur_pos = 0;
          do
          {
            // write MAX_BATCH_DATA_LENGTH(8M) to tfs cluster
            int64_t write_length = min(left_length, MAX_BATCH_DATA_LENGTH);
            FragInfo frag_info;
            int64_t real_write_length = write_data(ns_addr, cluster_id, reinterpret_cast<const char*>(buffer) + cur_pos,
                cur_offset, write_length, frag_info);
            if (real_write_length != write_length)
            {
              TBSYS_LOG(ERROR, "write tfs data error, cur_pos: %"PRI64_PREFIX"d"
                  "write_length(%"PRI64_PREFIX"d) => real_length(%"PRI64_PREFIX"d)",
                  cur_pos, write_length, real_write_length);
              unlink_file(frag_info, ns_addr);
              break;
            }
            TBSYS_LOG(DEBUG, "write tfs data, cluster_id: %d, cur_offset: %"PRI64_PREFIX"d, write_length: %"PRI64_PREFIX"d",
                cluster_id, cur_offset, write_length);
            frag_info.dump();

            // then write to meta server
            int32_t retry = 3;
            do
            {
              if ((ret = do_write(meta_server_id, app_id, uid, file_path, frag_info)) != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "write meta info error, cur_pos: %"PRI64_PREFIX"d, "
                    "write_length(%"PRI64_PREFIX"d) => real_length(%"PRI64_PREFIX"d), ret: %"PRI64_PREFIX"d",
                    cur_pos, write_length, real_write_length, ret);
              }
            }
            while(ret == EXIT_NETWORK_ERROR && --retry);

            if (need_update_table(ret))
            {
              update_table_from_rootserver();
            }

            if (ret != TFS_SUCCESS)
            {
              unlink_file(frag_info, ns_addr);
              break;
            }
            cur_pos += real_write_length;
            cur_offset += (offset == -1 ? 0 : real_write_length);
            left_length -= real_write_length;
          }
          while(left_length > 0);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = length - left_length;
        }
      }
      return ret;
    }

    int64_t NameMetaClientImpl::save_file(const char* ns_addr, int64_t app_id, int64_t uid, const char* local_file, const char* tfs_name)
    {
      int ret = TFS_ERROR;
      int fd = -1;
      int64_t read_len = 0, write_len = 0, pos = 0, off_set = 0;
      if (!is_valid_file_path(tfs_name))
      {
        TBSYS_LOG(ERROR, "file name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if (NULL == local_file)
      {
        TBSYS_LOG(ERROR, "local file is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else if ((fd = ::open(local_file, O_RDONLY)) < 0)
      {
        TBSYS_LOG(ERROR, "open local file %s fail: %s", local_file, strerror(errno));
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else if (TFS_SUCCESS != (ret = create_file(app_id, uid, tfs_name)))
      {
        TBSYS_LOG(ERROR, "create file error ret is %d", ret);
      }
      else
      {
        char* buf = new char[MAX_BATCH_DATA_LENGTH];
        //if save file we will delete the tfs_file

        while (TFS_SUCCESS == ret)
        {
          if ((read_len = ::read(fd, buf, MAX_BATCH_DATA_LENGTH)) < 0)
          {
            ret = EXIT_INVALID_ARGU_ERROR;
            TBSYS_LOG(ERROR, "read local file %s fail, error: %s", local_file, strerror(errno));
            break;
          }

          if (0 == read_len)
          {
            break;
          }
          pos = 0;
          while (read_len > 0)
          {
            write_len = write(ns_addr, app_id, uid, tfs_name, buf, off_set, read_len);
            if (write_len <= 0)
            {
              TBSYS_LOG(ERROR, "write to tfs fail");
              ret = write_len;
              break;
            }
            off_set += write_len;
            pos += write_len;
            read_len -= write_len;
          }
        }

        if (TFS_SUCCESS != ret)
        {
          rm_file(ns_addr, app_id, uid, tfs_name);
        }
        tbsys::gDeleteA(buf);
      }

      if (fd > 0)
      {
        ::close(fd);
      }

      return ret != TFS_SUCCESS ? ret : off_set;
    }

    int64_t NameMetaClientImpl::fetch_file(const char* ns_addr, int64_t app_id, int64_t uid, const char* local_file, const char* tfs_name)
    {
      int ret = TFS_ERROR;
      int fd = -1;
      int64_t off_set = 0;
      if (NULL == local_file)
      {
        TBSYS_LOG(ERROR, "local file is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else if (NULL == tfs_name)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if ((fd = ::open(local_file, O_WRONLY|O_CREAT, 0644)) < 0)
      {
        TBSYS_LOG(ERROR, "open local file %s to write fail: %s", local_file, strerror(errno));
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        int32_t io_size = MAX_READ_SIZE;

        char* buf = new char[io_size];
        int64_t read_len = 0, write_len = 0;
        ret = TFS_SUCCESS;
        while (1)
        {
          if ((read_len = read(ns_addr, app_id, uid, tfs_name, buf, off_set, io_size)) < 0)
          {
            ret = EXIT_INVALID_ARGU_ERROR;
            TBSYS_LOG(ERROR, "read tfs file fail. tfsname: %s", tfs_name);
            break;
          }

          if (0 == read_len)
          {
            break;
          }

          if ((write_len = ::write(fd, buf, read_len)) != read_len)
          {
            TBSYS_LOG(ERROR, "write local file %s fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d, error: %s",
                local_file, read_len, write_len, strerror(errno));
            ret = write_len;
            break;
          }
          off_set += read_len;
        }
        tbsys::gDeleteA(buf);
        ::close(fd);
      }

      return ret != TFS_SUCCESS ? ret : off_set;
    }

    int NameMetaClientImpl::read_frag_info(const int64_t app_id, const int64_t uid,
        const char* file_path, FragInfo& frag_info)
    {
      uint64_t meta_server_id = get_meta_server_id(app_id, uid);
      return read_frag_info(meta_server_id, app_id, uid, file_path, frag_info);
    }


    int NameMetaClientImpl::do_file_action(const int64_t app_id, const int64_t uid, MetaActionOp action, const char* path, const char* new_path)
    {
      int ret = TFS_ERROR;
      uint64_t meta_server_id = get_meta_server_id(app_id, uid);
      if (!is_valid_file_path(path) || ((new_path != NULL) && !is_valid_file_path(new_path)))
      {
        TBSYS_LOG(WARN, "path is invalid, old_path: %s, new_path: %s", path == NULL? "null":path, new_path == NULL? "null":new_path);
      }
      else if ((new_path != NULL) && (strcmp(path, new_path) == 0))
      {
        TBSYS_LOG(WARN, "source file path equals to destination file path: %s == %s", path, new_path);
      }
      else
      {
        int32_t retry = 3;
        do
        {
          if ((ret = NameMetaHelper::do_file_action(meta_server_id, app_id, uid, action, path, new_path, meta_table_.version_id_)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "do file action error occured, path: %s, new_path: %s, action: %d, ret: %d, retry: %d",
                path, (new_path == NULL? "null":new_path), action, ret, retry);
          }
        }
        while (ret == EXIT_NETWORK_ERROR && --retry);

        if (need_update_table(ret))
        {
          update_table_from_rootserver();
        }
      }
      return ret;
    }

    int NameMetaClientImpl::ls_dir(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* dir_path, int64_t pid, vector<FileMetaInfo>& v_file_meta_info, bool is_recursive)
    {
      int ret = TFS_SUCCESS;
      v_file_meta_info.clear();
      if ((ret = do_ls_ex(meta_server_id, app_id, uid, dir_path, DIRECTORY, pid, v_file_meta_info)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "ls directory failed, dir_path: %s", dir_path);
      }
      else if (is_recursive)
      {
        vector<FileMetaInfo>::iterator iter = v_file_meta_info.begin();
        vector<FileMetaInfo> sub_v_file_meta_info;
        sub_v_file_meta_info.clear();
        for (; iter != v_file_meta_info.end(); iter++)
        {
          if (!(iter->is_file()))
          {
            vector<FileMetaInfo> tmp_v_file_meta_info;
            tmp_v_file_meta_info.clear();
            if ((ret = ls_dir(meta_server_id, app_id, uid, NULL, iter->id_, tmp_v_file_meta_info, is_recursive)) != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "ls sub directory failed, pid: %"PRI64_PREFIX"d", iter->pid_);
              break;
            }
            if (static_cast<int32_t>(tmp_v_file_meta_info.size()) <= 0)
            {
              break;
            }
            vector<FileMetaInfo>::iterator tmp_iter = tmp_v_file_meta_info.begin();
            for (; tmp_iter != tmp_v_file_meta_info.end(); tmp_iter++)
            {
              sub_v_file_meta_info.push_back(*tmp_iter);
            }
          }
        }
        vector<FileMetaInfo>::iterator s_iter = sub_v_file_meta_info.begin();
        for (; s_iter != sub_v_file_meta_info.end(); s_iter++)
        {
          v_file_meta_info.push_back(*s_iter);
        }
      }

      return ret;
    }

    int NameMetaClientImpl::do_ls_ex(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* file_path, const FileType file_type, const int64_t pid,
        std::vector<FileMetaInfo>& v_file_meta_info, bool is_chk_exist)
    {
      int ret = TFS_ERROR;

      if (pid != -1 && !is_valid_file_path(file_path))
      {
        TBSYS_LOG(ERROR, "pid and file_path is invalid");
        return ret;
      }

      int32_t meta_size = 0;
      bool still_have = false;
      int64_t last_pid = pid;
      FileType last_file_type = file_type;
      char last_file_path[MAX_FILE_PATH_LEN];
      memset(last_file_path, 0, MAX_FILE_PATH_LEN);
      if (file_path != NULL)
      {
        strcpy(last_file_path, file_path);
      }

      do
      {
        std::vector<MetaInfo> tmp_v_meta_info;
        std::vector<MetaInfo>::iterator iter;

        int32_t retry = 3;
        do
        {
          if ((ret = NameMetaHelper::do_ls(meta_server_id, app_id, uid, last_file_path, last_file_type, last_pid,
                  meta_table_.version_id_, tmp_v_meta_info, still_have)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "do ls info failed, file_path: %s, file_type: %d, ret: %d, retry: %d",
                last_file_path, last_file_type, ret, retry);
          }
        }
        while (ret == EXIT_NETWORK_ERROR && --retry);

        if (need_update_table(ret))
        {
          update_table_from_rootserver();
        }

        if (ret != TFS_SUCCESS)
        {
          break;
        }

        meta_size = static_cast<int32_t>(tmp_v_meta_info.size());
        if (meta_size <= 0)
        {
          TBSYS_LOG(WARN, "no meta info returns");
          break;
        }

        if (file_type == NORMAL_FILE)
        {
          v_file_meta_info.push_back(tmp_v_meta_info[0].file_info_);
          break;
        }
        else
        {
          for (iter = tmp_v_meta_info.begin(); iter != tmp_v_meta_info.end(); iter++)
          {
            v_file_meta_info.push_back(iter->file_info_);
          }

          FileMetaInfo last_file_meta_info = tmp_v_meta_info[meta_size - 1].file_info_;
          last_pid = last_file_meta_info.pid_;
          last_file_type = last_file_meta_info.is_file() ? NORMAL_FILE:DIRECTORY;
          strcpy(last_file_path, last_file_meta_info.name_.c_str());
        }
      }
      while (meta_size > 0 && still_have && !is_chk_exist);
      return ret;
    }

    int NameMetaClientImpl::do_read(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* path, const int64_t offset, const int64_t size,
        FragInfo& frag_info, bool& still_have)
    {
      return NameMetaHelper::do_read_file(meta_server_id, app_id, uid,
          path, offset, size, meta_table_.version_id_, frag_info, still_have);
    }

    int NameMetaClientImpl::do_write(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* path, FragInfo& frag_info)
    {
      return NameMetaHelper::do_write_file(meta_server_id, app_id, uid, path, meta_table_.version_id_, frag_info);
    }

    bool NameMetaClientImpl::is_valid_file_path(const char* file_path)
    {
      return ((file_path != NULL) && (strlen(file_path) > 0) && (static_cast<int32_t>(strlen(file_path)) < MAX_FILE_PATH_LEN) && (file_path[0] == '/') && (strstr(file_path, " ") == NULL));
    }

    uint64_t NameMetaClientImpl::get_meta_server_id(const int64_t app_id, const int64_t uid)
    {
      uint64_t meta_server_id = 0;
      HashHelper helper(app_id, uid);
      uint32_t hash_value = tbsys::CStringUtil::murMurHash((const void*)&helper, sizeof(HashHelper));
      {
        tbsys::CRLockGuard guard(meta_table_mutex_);
        int32_t table_size = meta_table_.v_meta_table_.size();
        if (table_size <= 0)
        {
          meta_server_id = 0;
          TBSYS_LOG(WARN, "no meta server is available");
        }
        else
        {
          meta_server_id = meta_table_.v_meta_table_.at(hash_value % table_size);
        }
      }
      return meta_server_id;
    }

    int NameMetaClientImpl::read_frag_info(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* file_path, FragInfo& frag_info)
    {
      int ret = TFS_SUCCESS;
      if (NULL == file_path)
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        bool still_have = true;
        FragInfo tmp_frag_info;
        int64_t offset = 0;
        do
        {
          tmp_frag_info.v_frag_meta_.clear();
          int32_t retry = 3;
          do
          {
            if ((ret = NameMetaHelper::do_read_file(meta_server_id, app_id, uid, file_path,
                    offset, MAX_READ_FRAG_SIZE, meta_table_.version_id_, tmp_frag_info, still_have)) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "read frag info error, ret: %d, retry: %d", ret, retry);
            }
          }
          while(ret == EXIT_NETWORK_ERROR && --retry);

          if (need_update_table(ret))
          {
            update_table_from_rootserver();
          }

          offset = tmp_frag_info.get_last_offset();
          frag_info.cluster_id_ = tmp_frag_info.cluster_id_;
          frag_info.push_back(tmp_frag_info);
        }
        while ((offset != 0) && still_have);
      }
      return ret;
    }

    int32_t NameMetaClientImpl::get_cluster_id(const int64_t app_id, const int64_t uid, const char* path)
    {
      uint64_t meta_server_id = get_meta_server_id(app_id, uid);
      return get_cluster_id(meta_server_id, app_id, uid, path);
    }
    int32_t NameMetaClientImpl::get_cluster_id(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
        const char* path)
    {
      FragInfo frag_info;
      bool still_have = false;
      int ret = TFS_SUCCESS;
      int32_t cluster_id = -1;
      int32_t retry = 3;
      do
      {
        if ((ret = do_read(meta_server_id, app_id, uid, path, 0, 0, frag_info, still_have)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get cluster id failed, ret: %d, retry: %d", ret, retry);
        }
      }
      while (ret == EXIT_NETWORK_ERROR && --retry);

      if (need_update_table(ret))
      {
        update_table_from_rootserver();
      }

      if (TFS_SUCCESS == ret)
      {
        cluster_id = frag_info.cluster_id_;
      }
      return cluster_id;
    }

    int NameMetaClientImpl::unlink_file(FragInfo& frag_info, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      int tmp_ret = TFS_ERROR;
      int64_t file_size = 0;
      std::vector<FragMeta>::iterator iter = frag_info.v_frag_meta_.begin();
      for(; iter != frag_info.v_frag_meta_.end(); iter++)
      {
        FSName fsname(iter->block_id_, iter->file_id_, frag_info.cluster_id_);
        if ((tmp_ret = TfsClient::Instance()->unlink(file_size, fsname.get_name(), NULL, ns_addr)) != TFS_SUCCESS)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "unlink tfs file failed, file: %s, ret: %d", fsname.get_name(), tmp_ret);
        }
      }
      return ret;
    }

    int64_t NameMetaClientImpl::read_data(const char* ns_addr, const FragInfo& frag_info, void* buffer, int64_t offset, int64_t length)
    {
      int64_t ret = TFS_SUCCESS;
      int64_t cur_offset = offset;
      int64_t cur_length = 0;
      int64_t left_length = length;
      int64_t cur_pos = 0;

      const vector<FragMeta>& v_frag_meta = frag_info.v_frag_meta_;
      vector<FragMeta>::const_iterator iter = v_frag_meta.begin();
      for(; iter != v_frag_meta.end(); iter++)
      {
        if (cur_offset > iter->offset_ + iter->size_)
        {
          TBSYS_LOG(ERROR, "fatal error wrong pos, cur_offset: %"PRI64_PREFIX"d, total: %"PRI64_PREFIX"d",
              cur_offset, (iter->offset_ + iter->size_));
          break;
        }

        // deal with the hole
        if (cur_offset < iter->offset_)
        {
          int32_t diff = min(offset + length - cur_offset, iter->offset_ - cur_offset);
          left_length -= diff;
          memset(reinterpret_cast<char*>(buffer) + cur_pos, 0, diff);
          if (left_length <= 0)
          {
            left_length = 0;
            break;
          }
          cur_offset += diff;
          cur_pos += diff;
        }

        cur_length = min(iter->size_ - (cur_offset - iter->offset_), left_length);
        int64_t read_length = tfs_meta_manager_.read_data(ns_addr, iter->block_id_, iter->file_id_,
            reinterpret_cast<char*>(buffer) + cur_pos, (cur_offset - iter->offset_), cur_length);

        if (read_length < 0)
        {
          TBSYS_LOG(ERROR, "read tfs data failed, block_id: %u, file_id: %"PRI64_PREFIX"u",
              iter->block_id_, iter->file_id_);
          ret = read_length;
          break;
        }

        if (read_length != cur_length)
        {
          left_length -= read_length;
          TBSYS_LOG(WARN, "read tfs data return wrong length,"
              " cur_offset: %"PRI64_PREFIX"d, cur_length: %"PRI64_PREFIX"d, "
              "read_length(%"PRI64_PREFIX"d) => cur_length(%"PRI64_PREFIX"d), left_length: %"PRI64_PREFIX"d",
              cur_offset, cur_length, read_length, cur_length, left_length);
          frag_info.dump();
          break;
        }
        cur_pos += read_length;
        cur_offset += read_length;
        left_length -= read_length;

        if (left_length <= 0)
        {
          break;
        }
      }
      return (ret == TFS_SUCCESS) ? (length - left_length):ret;
    }

    int64_t NameMetaClientImpl::write_data(const char* ns_addr, int32_t cluster_id,
        const void* buffer, int64_t offset, int64_t length,
        FragInfo& frag_info)
    {
      int64_t ret = TFS_SUCCESS;
      int64_t write_length = 0;
      int64_t left_length = length;
      int64_t cur_offset = offset;
      int64_t cur_pos = 0;

      if (cluster_id != 0)
      {
        frag_info.cluster_id_ = cluster_id;
      }
      else
      {
        frag_info.cluster_id_ = tfs_meta_manager_.get_cluster_id(ns_addr);
      }
      do
      {
        // write to tfs, and get frag meta
        write_length = min(left_length, MAX_SEGMENT_LENGTH);
        FragMeta frag_meta;
        int64_t real_length = tfs_meta_manager_.write_data(ns_addr, reinterpret_cast<const char*>(buffer) + cur_pos, cur_offset, write_length, frag_meta);

        if (real_length != write_length)
        {
          TBSYS_LOG(ERROR, "write segment data failed, cur_pos : %"PRI64_PREFIX"d, write_length : %"PRI64_PREFIX"d, ret_length: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
              cur_pos, write_length, real_length, ret);
          if (real_length < 0)
          {
            ret = real_length;
          }
          break;
        }
        else
        {
          frag_info.v_frag_meta_.push_back(frag_meta);
          cur_offset += (offset == -1 ? 0 : real_length);
          cur_pos += real_length;
          left_length -= real_length;
        }
      }
      while(left_length > 0);
      if (TFS_SUCCESS == ret)
      {
        ret = length - left_length;
      }
      return ret;
    }

    bool NameMetaClientImpl::need_update_table(const int ret_status)
    {
      return (ret_status == EXIT_TABLE_VERSION_ERROR
          || ret_status == EXIT_LEASE_EXPIRED
          || ret_status == EXIT_NETWORK_ERROR);
    }

    int NameMetaClientImpl::update_table_from_rootserver()
    {
      int ret = TFS_ERROR;
      char table_info[MAX_BUCKET_DATA_LENGTH];
      int64_t version_id = -1;
      uint64_t table_length = common::MAX_BUCKET_DATA_LENGTH;
      if ((ret = NameMetaHelper::get_table(rs_id_, table_info, table_length, version_id)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get tables from rootserver failed. ret: %d", ret);
      }
      else
      {
        int64_t table_size = table_length / INT64_SIZE;
        if (table_size != common::MAX_BUCKET_ITEM_DEFAULT)
        {
          TBSYS_LOG(ERROR, "tables size is not correct. table size: %"PRI64_PREFIX"d", table_size);
        }
        else
        {
          tbsys::CWLockGuard guard(meta_table_mutex_);
          meta_table_.v_meta_table_.clear();
          uint64_t* meta_servers = reinterpret_cast<uint64_t*> (table_info);
          meta_table_.v_meta_table_.assign(meta_servers, meta_servers + table_size);
          meta_table_.version_id_ = version_id;
          //meta_table_.dump();
          ret = TFS_SUCCESS;
        }
      }

      return ret;
    }

    int NameMetaClientImpl::get_parent_dir(char* dir_path, char*& parent_dir_path)
    {
      int ret = TFS_SUCCESS;
      // trim tailing "/"
      dir_path = tbsys::CStringUtil::trim(dir_path, "/", 2);

      if (strlen(dir_path) > 1)
      {
       char *last_slash = strrchr(dir_path, '/');
        if (NULL != last_slash)
        {
           *(last_slash + 1) = '\0';
        }
        parent_dir_path = dir_path;
      }
      // case of "/"
      else if (0 == strlen(dir_path))
      {
        parent_dir_path = NULL;
      }
      else
      {
        ret = EXIT_INVALID_FILE_NAME;
      }
      return ret;
    }

  }
}
