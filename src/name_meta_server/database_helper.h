/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 */

#ifndef TFS_NAMEMETASERVER_DATABASE_HELPER_H_
#define TFS_NAMEMETASERVER_DATABASE_HELPER_H_
#include "common/internal.h"
#include "common/meta_server_define.h"

namespace tfs
{
  namespace namemetaserver
  {
    class DatabaseHelper
    {
      public:
        DatabaseHelper();
        virtual ~DatabaseHelper();

        bool is_connected() const;
        int set_conn_param(const char* conn_str, const char* user_name, const char* passwd);

        virtual int connect() = 0;
        virtual int close() = 0;

        virtual int ls_meta_info(std::vector<common::MetaInfo>& out_v_meta_info,
            const int64_t app_id, const int64_t uid,
            const int64_t pid = 0, const char* name = NULL, const int32_t name_len = 0,
            const char* name_end = NULL, const int32_t name_end_len = 0) = 0;

        virtual int create_dir(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
            int64_t& mysql_proc_ret) = 0;

        virtual int rm_dir(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int32_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret) = 0;

        virtual int mv_dir(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len, int64_t& mysql_proc_ret) = 0;

        virtual int create_file(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret) = 0;

        virtual int rm_file(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret) = 0;

        virtual int pwrite_file(const int64_t app_id, const int64_t uid,
            const int64_t pid, const char* name, const int32_t name_len,
            const int64_t size, const int16_t ver_no, const char* meta_info, const int32_t meta_len,
            int64_t& mysql_proc_ret) = 0;

        virtual int mv_file(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len, int64_t& mysql_proc_ret) = 0;

        virtual int get_nex_val(int64_t& next_val) = 0;

      protected:
        enum
        {
          CONN_STR_LEN = 256,
        };
        char conn_str_[CONN_STR_LEN];
        char user_name_[CONN_STR_LEN];
        char passwd_[CONN_STR_LEN];
        bool is_connected_;

      private:
        DISALLOW_COPY_AND_ASSIGN(DatabaseHelper);
    };
  }
}
#endif
