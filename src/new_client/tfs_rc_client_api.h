/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.h 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_RC_CLIENTAPI_H_
#define TFS_CLIENT_RC_CLIENTAPI_H_

#include <string>
#include "common/define.h"
#include "common/meta_server_define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class RcClientImpl;
    class RcClient
    {
      public:
        enum RC_MODE
        {
          CREATE = 1,
          READ = 2,
          //STAT = 3,
          WRITE = 4, //raw tfs will not use this. only CREATE.
                     //this for name meta tfs.
                     //use create create a tfs_file, use write for writing or appendding
          READ_FORCE = 8
        };
      public:
        RcClient();
        ~RcClient();

        TfsRetType initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip = NULL,
            const int32_t cache_times = -1,
            const int32_t cache_items = -1,
            const char* dev_name = NULL);
        TfsRetType initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip = 0,
            const int32_t cache_times = -1,
            const int32_t cache_items = -1,
            const char* dev_name = NULL);

        int64_t get_app_id() const;

        // for imgsrc tmp use
        void set_remote_cache_info(const char * remote_cache_info);

        void set_client_retry_count(const int64_t count);
        int64_t get_client_retry_count() const;
        void set_client_retry_flag(bool retry_flag = true);

        void set_wait_timeout(const int64_t timeout_ms);
        void set_log_level(const char* level);
        void set_log_file(const char* log_file);

        TfsRetType logout();

        // for raw tfs
        int open(const char* file_name, const char* suffix, const RC_MODE mode,
            const bool large = false, const char* local_key = NULL);
        TfsRetType close(const int fd, char* tfs_name_buff = NULL, const int32_t buff_len = 0);

        int64_t read(const int fd, void* buf, const int64_t count);
        int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);

        int64_t write(const int fd, const void* buf, const int64_t count);

        int64_t lseek(const int fd, const int64_t offset, const int whence);
        TfsRetType fstat(const int fd, common::TfsFileStat* buf);

        TfsRetType unlink(const char* file_name, const char* suffix = NULL,
            const common::TfsUnlinkType action = common::DELETE);
        int64_t save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
            const char* suffix = NULL, const bool is_large_file = false);
        int64_t save_buf(const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len, const char* suffix = NULL);
        int fetch_file(const char* local_file,
                       const char* file_name, const char* suffix = NULL);
        int fetch_buf(int64_t& ret_count, char* buf, const int64_t count,
                     const char* file_name, const char* suffix = NULL);


        // for name meta
        TfsRetType create_dir(const int64_t uid, const char* dir_path);
        TfsRetType create_dir_with_parents(const int64_t uid, const char* dir_path);
        TfsRetType create_file(const int64_t uid, const char* file_path);

        TfsRetType rm_dir(const int64_t uid, const char* dir_path);
        TfsRetType rm_file(const int64_t uid, const char* file_path);

        TfsRetType mv_dir(const int64_t uid,
            const char* src_dir_path, const char* dest_dir_path);
        TfsRetType mv_file(const int64_t uid,
            const char* src_file_path, const char* dest_file_path);

        TfsRetType ls_dir(const int64_t app_id, const int64_t uid,
            const char* dir_path,
            std::vector<common::FileMetaInfo>& v_file_meta_info);
        TfsRetType ls_file(const int64_t app_id, const int64_t uid,
            const char* file_path,
            common::FileMetaInfo& file_meta_info);

        bool is_dir_exist(const int64_t app_id, const int64_t uid, const char* dir_path);
        bool is_file_exist(const int64_t app_id, const int64_t uid, const char* file_path);

        int open(const int64_t app_id, const int64_t uid, const char* name, const RcClient::RC_MODE mode);
        int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
        // when do append operation, set offset = -1
        int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);

        //close use the same close func as raw tfs


        /**
         * @brief saved local_file to tfs, named tfs_file_name
         *
         * @param uid: user id
         * @param local_file: local file name
         * @param tfs_file_name: tfs file name
         *
         * @return data length saved
         */
        int64_t save_file(const int64_t uid,
            const char* local_file, const char* tfs_file_name);

        /**
         * @brief fetch tfs file nemed tfs_file_name, stored to local_file
         *
         * @param app_id: app id
         * @param uid: user id
         * @param local_file: local file name
         * @param tfs_file_name: tfs file name
         *
         * @return data length fetched
         */
        int64_t fetch_file(const int64_t app_id, const int64_t uid,
            const char* local_file, const char* tfs_file_name);

        /**
         * @brief save buf as tfs file named tfs_file_name
         *
         * @param uid: user id
         * @param buf: buf pointer
         * @param buf_len: buf length
         * @param tfs_file_name: tfs file name
         *
         * @return data length saved
         */
        int64_t save_buf(const int64_t uid,
            const char* buf, const int32_t buf_len, const char* tfs_file_name);


        /**
        * @brief fetch tfs file and store to buffer
        *
        * @param app_id: app id
        * @param uid: user id
        * @param buffer: buffer to store file content
        * @param offset: file offset to read start
        * @param length: length to read
        * @param tfs_file_name: tfs file name
        *
        * @return
        */
        int64_t fetch_buf(const int64_t app_id, const int64_t uid,
          char* buffer, const int64_t offset, const int64_t length, const char* tfs_file_name);

      private:
        RcClient(const RcClient&);
        RcClientImpl* impl_;
    };
  }
}

#endif
