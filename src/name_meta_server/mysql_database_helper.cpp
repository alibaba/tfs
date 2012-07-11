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
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include "mysql_database_helper.h"

#include <errmsg.h>
#include <vector>
#include "common/define.h"
#include "common/func.h"

using namespace std;
namespace tfs
{
  namespace namemetaserver
  {
    using namespace common;
    MysqlDatabaseHelper::MysqlDatabaseHelper()
    {
      stmt_ = NULL;
    }
    MysqlDatabaseHelper::~MysqlDatabaseHelper()
    {
      close();
      mysql_server_end();
    }
    int MysqlDatabaseHelper::connect()
    {
      int ret = TFS_SUCCESS;
      if (is_connected_)
      {
        close();
      }

      if (!init_mysql(conn_str_, user_name_, passwd_))
      {
        ret = TFS_ERROR;
      }
      else if (!open_mysql())
      {
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        char sql[1024];
        snprintf(sql, 1024, "select pid, name, id, UNIX_TIMESTAMP(create_time), "
            "UNIX_TIMESTAMP(modify_time), size, ver_no, meta_info from t_meta_info "
            "where app_id = ? and uid = ? and pid = ? and name >= ? and name < ? limit %d", ROW_LIMIT);
        if (NULL != stmt_)
        {
          mysql_stmt_free_result(stmt_);
          mysql_stmt_close(stmt_);
        }
        stmt_ = mysql_stmt_init(&mysql_.mysql);
        int status = mysql_stmt_prepare(stmt_, sql, strlen(sql));
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params_, 0, sizeof (ps_params_));
          ps_params_[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params_[0].buffer = (char *) &app_id_;
          ps_params_[0].length = 0;
          ps_params_[0].is_null = 0;

          ps_params_[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params_[1].buffer = (char *) &uid_;
          ps_params_[1].length = 0;
          ps_params_[1].is_null = 0;

          ps_params_[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params_[2].buffer = (char *) &pid_;
          ps_params_[2].length = 0;
          ps_params_[2].is_null = 0;
          ps_params_[2].is_unsigned = 1;

          ps_params_[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params_[3].buffer = (char *) pname_;
          ps_params_[3].length = &pname_len_;
          ps_params_[3].is_null = 0;

          ps_params_[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params_[4].buffer = (char *) pname_end_;
          ps_params_[4].length = &pname_end_len_;
          ps_params_[4].is_null = 0;

          status = mysql_stmt_bind_param(stmt_, ps_params_);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
            ret = TFS_ERROR;
          }
        }
      }

      is_connected_ = TFS_SUCCESS == ret;
      return ret;
    }

    int MysqlDatabaseHelper::close()
    {
      if (NULL != stmt_)
      {
        mysql_stmt_free_result(stmt_);
        mysql_stmt_close(stmt_);
        stmt_ = NULL;
      }
      close_mysql();
      is_connected_ = false;
      return TFS_SUCCESS;
    }
    int MysqlDatabaseHelper::ls_meta_info(std::vector<MetaInfo>& out_v_meta_info,
        const int64_t app_id, const int64_t uid,
            const int64_t pid, const char* name, const int32_t name_len, const char* name_end, const int32_t name_end_len)
    {
      int ret = TFS_ERROR;
      out_v_meta_info.clear();
      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        ret = TFS_SUCCESS;
        int status;
        app_id_ = app_id;
        uid_ = uid;
        pid_ = pid;
        if (NULL != name && name_len > 0 && name_len < MAX_FILE_PATH_LEN)
        {
          memcpy(pname_, name, name_len);
          pname_len_ = name_len;
        }
        else
        {
          pname_[0] = 0;
          pname_len_ = 1;
        }
        if (NULL != name_end && name_end_len > 0 && name_end_len < MAX_FILE_PATH_LEN)
        {
          memcpy(pname_end_, name_end, name_end_len);
          pname_end_len_ = name_end_len;

        }
        else
        {
          pname_end_[0]= 0xff;
          pname_end_len_ = 1;
        }
        status = mysql_stmt_execute(stmt_);
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt_) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          MYSQL_BIND rs_bind[8];  /* for output buffers */
          my_bool    is_null[8];
          int64_t o_pid = 0;
          char o_name[MAX_FILE_PATH_LEN];
          unsigned long o_name_len = 0;
          int64_t o_id = 0;
          int32_t o_create_time = 0;
          int32_t o_modify_time = 0;
          int64_t o_size = 0;
          int16_t o_ver_no = 0;
          char o_slide_info[MAX_FRAG_INFO_SIZE];
          unsigned long o_slide_info_len = 0;

          memset(rs_bind, 0, sizeof (rs_bind) );

          /* set up and bind result set output buffers */
          rs_bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
          rs_bind[0].is_null = &is_null[0];
          rs_bind[0].buffer = (char *) &o_pid;
          rs_bind[0].is_unsigned = 1;

          rs_bind[1].buffer_type = MYSQL_TYPE_STRING;
          rs_bind[1].is_null = &is_null[1];
          rs_bind[1].buffer = (char *) o_name;
          rs_bind[1].buffer_length = MAX_FILE_PATH_LEN;
          rs_bind[1].length= &o_name_len;

          rs_bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
          rs_bind[2].is_null = &is_null[2];
          rs_bind[2].buffer = (char *) &o_id;

          rs_bind[3].buffer_type = MYSQL_TYPE_LONG;
          rs_bind[3].is_null = &is_null[3];
          rs_bind[3].buffer = (char *) &o_create_time;

          rs_bind[4].buffer_type = MYSQL_TYPE_LONG;
          rs_bind[4].is_null = &is_null[4];
          rs_bind[4].buffer = (char *) &o_modify_time;

          rs_bind[5].buffer_type = MYSQL_TYPE_LONGLONG;
          rs_bind[5].is_null = &is_null[5];
          rs_bind[5].buffer = (char *) &o_size;

          rs_bind[6].buffer_type = MYSQL_TYPE_SHORT;
          rs_bind[6].is_null = &is_null[6];
          rs_bind[6].buffer = (char *) &o_ver_no;

          rs_bind[7].buffer_type = MYSQL_TYPE_BLOB;
          rs_bind[7].is_null = &is_null[7];
          rs_bind[7].buffer_length = MAX_FRAG_INFO_SIZE;
          rs_bind[7].buffer = (char *) o_slide_info;
          rs_bind[7].length= &o_slide_info_len;

          status = mysql_stmt_bind_result(stmt_, rs_bind);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            status = mysql_stmt_store_result(stmt_);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
              ret = TFS_ERROR;
            }
          }
          while (TFS_SUCCESS == ret )
          {
            status = mysql_stmt_fetch(stmt_);
            if (1 == status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt_), mysql_stmt_errno(stmt_));
              ret = TFS_ERROR;
              break;
            }
            else if (MYSQL_NO_DATA == status)
            {
              break;
            }
            else if (MYSQL_DATA_TRUNCATED == status)
            {
              TBSYS_LOG(ERROR, "MYSQL_DATA_TRUNCATED");
              break;
            }
            out_v_meta_info.push_back(MetaInfo());
            std::vector<MetaInfo>::iterator iter = out_v_meta_info.end() - 1;
            (*iter).file_info_.name_.assign(o_name, o_name_len);
            (*iter).file_info_.pid_ = o_pid;
            (*iter).file_info_.id_ = o_id;
            (*iter).file_info_.create_time_ = o_create_time;
            (*iter).file_info_.modify_time_ = o_modify_time;
            (*iter).file_info_.size_ = o_size;
            (*iter).file_info_.ver_no_ = o_ver_no;
            if (!is_null[7])
            {
              int64_t pos = 0;
              (*iter).frag_info_.deserialize(o_slide_info, o_slide_info_len, pos);
            }
          }
          mysql_next_result(&mysql_.mysql); //mysql bugs, we must have this
        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }

    int MysqlDatabaseHelper::create_dir(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
        int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[7];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL create_dir(?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;
      int retry_time = 0;

      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[3].buffer = (char *) pname;
          ps_params[3].length = &_pname_len;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[4].buffer = (char *) &pid;
          ps_params[4].length = 0;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &id;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[6].buffer = (char *) name;
          ps_params[6].length = &_name_len;
          ps_params[6].is_null = 0;

          //ps_params[7].buffer_type = MYSQL_TYPE_LONG;
          //ps_params[7].buffer = (char *) &mysql_proc_ret;
          //ps_params[7].length = 0;
          //ps_params[7].is_null = 0;

          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::rm_dir(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int32_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[7];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL rm_dir(?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[3].buffer = (char *) pname;
          ps_params[3].length = &_pname_len;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[4].buffer = (char *) &pid;
          ps_params[4].length = 0;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &id;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[6].buffer = (char *) name;
          ps_params[6].length = &_name_len;
          ps_params[6].is_null = 0;

          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }

    int MysqlDatabaseHelper::mv_dir(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[10];  /* input parameter buffers */
      unsigned long _s_pname_len = s_pname_len;
      unsigned long _d_pname_len = d_pname_len;
      unsigned long _s_name_len = s_name_len;
      unsigned long _d_name_len = d_name_len;
      int        status;
      const char* str = "CALL mv_dir(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &s_ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &s_pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) s_pname;
          ps_params[4].length = &_s_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &d_ppid;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[6].buffer = (char *) &d_pid;
          ps_params[6].length = 0;
          ps_params[6].is_null = 0;

          ps_params[7].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[7].buffer = (char *) d_pname;
          ps_params[7].length = &_d_pname_len;
          ps_params[7].is_null = 0;

          ps_params[8].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[8].buffer = (char *) s_name;
          ps_params[8].length = &_s_name_len;
          ps_params[8].is_null = 0;

          ps_params[9].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[9].buffer = (char *) d_name;
          ps_params[9].length = &_d_name_len;
          ps_params[9].is_null = 0;


          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::create_file(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[6];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL create_file(?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) pname;
          ps_params[4].length = &_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[5].buffer = (char *) name;
          ps_params[5].length = &_name_len;
          ps_params[5].is_null = 0;

          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::rm_file(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[6];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL rm_file(?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) pname;
          ps_params[4].length = &_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[5].buffer = (char *) name;
          ps_params[5].length = &_name_len;
          ps_params[5].is_null = 0;

          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::pwrite_file(const int64_t app_id, const int64_t uid,
        const int64_t pid, const char* name, const int32_t name_len,
        const int64_t size, const int16_t ver_no, const char* meta_info, const int32_t meta_len,
        int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      if (MAX_FRAG_INFO_SIZE >= meta_len)
      {
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[7];  /* input parameter buffers */
        unsigned long _name_len = name_len;
        unsigned long _meta_len = meta_len;
        int        status;
        const char* str = "CALL pwrite_file(?, ?, ?, ?, ?, ?, ?)";

        mysql_proc_ret = 0;

        int retry_time = 0;
        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
            ps_params[0].buffer = (char *) &app_id;
            ps_params[0].length = 0;
            ps_params[0].is_null = 0;

            ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
            ps_params[1].buffer = (char *) &uid;
            ps_params[1].length = 0;
            ps_params[1].is_null = 0;

            ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
            ps_params[2].buffer = (char *) &pid;
            ps_params[2].length = 0;
            ps_params[2].is_null = 0;

            ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
            ps_params[3].buffer = (char *) name;
            ps_params[3].length = &_name_len;
            ps_params[3].is_null = 0;

            ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
            ps_params[4].buffer = (char *) &size;
            ps_params[4].length = 0;
            ps_params[4].is_null = 0;

            ps_params[5].buffer_type = MYSQL_TYPE_SHORT;
            ps_params[5].buffer = (char *) &ver_no;
            ps_params[5].length = 0;
            ps_params[5].is_null = 0;

            ps_params[6].buffer_type = MYSQL_TYPE_BLOB;
            ps_params[6].buffer = (char *) meta_info;
            ps_params[6].length = &_meta_len;
            ps_params[6].is_null = 0;

            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              if (!excute_stmt(stmt, mysql_proc_ret))
              {
                ret = TFS_ERROR;
              }
            }

          }
        }
        if (TFS_SUCCESS != ret)
        {
          close();
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "meta len is %d too much", meta_len);
      }
      return ret;
    }
    int MysqlDatabaseHelper::mv_file(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        int64_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[10];  /* input parameter buffers */
      unsigned long _s_pname_len = s_pname_len;
      unsigned long _d_pname_len = d_pname_len;
      unsigned long _s_name_len = s_name_len;
      unsigned long _d_name_len = d_name_len;
      int        status;
      const char* str = "CALL mv_file(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &s_ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &s_pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) s_pname;
          ps_params[4].length = &_s_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &d_ppid;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[6].buffer = (char *) &d_pid;
          ps_params[6].length = 0;
          ps_params[6].is_null = 0;

          ps_params[7].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[7].buffer = (char *) d_pname;
          ps_params[7].length = &_d_pname_len;
          ps_params[7].is_null = 0;

          ps_params[8].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[8].buffer = (char *) s_name;
          ps_params[8].length = &_s_name_len;
          ps_params[8].is_null = 0;

          ps_params[9].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[9].buffer = (char *) d_name;
          ps_params[9].length = &_d_name_len;
          ps_params[9].is_null = 0;


          status = mysql_stmt_bind_param(stmt, ps_params);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!excute_stmt(stmt, mysql_proc_ret))
            {
              ret = TFS_ERROR;
            }
          }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::get_nex_val(int64_t& next_val)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      int        status;
      const char* str = "CALL pid_seq_nextval()";

      next_val = 0;

      int retry_time = 0;
      tbutil::Mutex::Lock lock(mutex_);
retry:
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str));
        if (status)
        {
          if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
          {
            close();
            goto retry;
          }
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
            if (!excute_stmt(stmt, next_val))
            {
              ret = TFS_ERROR;
            }

        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }

    bool MysqlDatabaseHelper::init_mysql(const char* mysqlconn, const char* user_name, const char* passwd)
    {
      vector<string> fields;
      tfs::common::Func::split_string(mysqlconn, ':', fields);
      mysql_.isopen = false;
      if (fields.size() < 3 || NULL == user_name || NULL == passwd)
        return false;
      mysql_.host = fields[0];
      mysql_.port = atoi(fields[1].c_str());
      mysql_.user = user_name;
      mysql_.pass = passwd;
      mysql_.database = fields[2];
      mysql_.inited = true;

      int v = 5;
      mysql_init(&mysql_.mysql);
      mysql_options(&mysql_.mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&v);
      mysql_options(&mysql_.mysql, MYSQL_OPT_READ_TIMEOUT, (const char *)&v);
      mysql_options(&mysql_.mysql, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&v);
      return true;
    }

    bool MysqlDatabaseHelper::open_mysql()
    {
      if (!mysql_.inited) return false;
      if (mysql_.isopen) return true;
      MYSQL *conn = mysql_real_connect(
          &mysql_.mysql,
          mysql_.host.c_str(),
          mysql_.user.c_str(),
          mysql_.pass.c_str(),
          mysql_.database.c_str(),
          mysql_.port, NULL, CLIENT_MULTI_STATEMENTS);
      if (!conn)
      {
        TBSYS_LOG(ERROR, "connect mysql database (%s:%d:%s:%s:%s) error(%s)",
            mysql_.host.c_str(), mysql_.port, mysql_.user.c_str(), mysql_.database.c_str(), mysql_.pass.c_str(),
            mysql_error(&mysql_.mysql));
        return false;
      }
      mysql_.isopen = true;
      return true;
    }

    int MysqlDatabaseHelper::close_mysql()
    {
      if (mysql_.isopen)
      {
        mysql_close(&mysql_.mysql);
      }
      return 0;
    }
    bool MysqlDatabaseHelper::excute_stmt(MYSQL_STMT *stmt, int64_t& mysql_proc_ret)
    {
      bool ret = true;
      int status;
      my_bool    is_null;    /* input parameter nullability */
      status = mysql_stmt_execute(stmt);
      if (status)
      {
        TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
            mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
        ret = false;
      }
      if (ret)
      {
        int num_fields;       /* number of columns in result */
        MYSQL_BIND rs_bind;  /* for output buffers */

        /* the column count is > 0 if there is a result set */
        /* 0 if the result is only the final status packet */
        num_fields = mysql_stmt_field_count(stmt);
        TBSYS_LOG(DEBUG, "num_fields = %d", num_fields);
        if (num_fields == 1)
        {

          memset(&rs_bind, 0, sizeof (MYSQL_BIND));

          /* set up and bind result set output buffers */
          rs_bind.buffer_type = MYSQL_TYPE_LONGLONG;
          rs_bind.is_null = &is_null;

          rs_bind.buffer = (char *) &mysql_proc_ret;
          rs_bind.buffer_length = sizeof(mysql_proc_ret);

          status = mysql_stmt_bind_result(stmt, &rs_bind);
          if (status)
          {
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = false;
          }
          while (ret)
          {
            status = mysql_stmt_fetch(stmt);

            if (status == MYSQL_NO_DATA)
            {
              break;
            }
            if (status == 1)
            {
              TBSYS_LOG(ERROR, "mysql_stmt_fetch error");
            }
            TBSYS_LOG(DEBUG, "mysql_proc_ret = %"PRI64_PREFIX"d", mysql_proc_ret);
          }
          mysql_next_result(&mysql_.mysql); //mysql bugs, we must have this
        }
        else
        {
          TBSYS_LOG(ERROR, "num_fields = %d have debug info in prcedure?", num_fields);
          ret = false;
        }

        mysql_stmt_free_result(stmt);
        mysql_stmt_close(stmt);
      }
      return ret;
    }
  }
}

