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

#include <mysql.h>
#include <errmsg.h>
#include <vector>
#include "common/define.h"
#include "common/func.h"

using namespace std;
namespace
{
struct mysql_ex {
    string host;
    int port;
    string user;
    string pass;
    string database;
    bool   isopen;
    bool   inited;
    MYSQL  mysql;
};

static mysql_ex  mysql_;
static bool init_mysql(const char* mysqlconn, const char* user_name, const char* passwd)
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

static bool open_mysql()
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

static int close_mysql()
{
    if (mysql_.isopen)
    {
        mysql_close(&mysql_.mysql);
    }
    return 0;
}
}

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
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

      is_connected_ = TFS_SUCCESS == ret;
      return ret;
    }

    int MysqlDatabaseHelper::close()
    {
      close_mysql();
      is_connected_ = false;
      return TFS_SUCCESS;
    }

    int MysqlDatabaseHelper::select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam)
    {
      UNUSED(inparam);
      UNUSED(outparam);
      //not supported
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::update(const ResourceServerInfo& inparam)
    {
      UNUSED(inparam);
      //not supported
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::remove(const ResourceServerInfo& inparam)
    {
      UNUSED(inparam);
      //not supported
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::scan(VResourceServerInfo& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_resource_server_info");
        snprintf(sql, 1024, "select addr_info, stat, rem from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        ResourceServerInfo tmp;
        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          snprintf(tmp.addr_info_, ADDR_INFO_LEN, "%s", row[0]);
          tmp.stat_ = atoi(row[1]);
          snprintf(tmp.rem_, REM_LEN, "%s", row[2]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::scan(VMetaRootServerInfo& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_meta_root_info");
        snprintf(sql, 1024, "select app_id, addr_info, stat, rem from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        MetaRootServerInfo tmp;
        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          tmp.app_id_ = atoi(row[0]);
          snprintf(tmp.addr_info_, ADDR_INFO_LEN, "%s", row[1]);
          tmp.stat_ = atoi(row[2]);
          snprintf(tmp.rem_, REM_LEN, "%s", row[3]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::select(const ClusterRackInfo& inparam, ClusterRackInfo& outparam)
    {
      UNUSED(inparam);
      UNUSED(outparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::update(const ClusterRackInfo& inparam)
    {
      UNUSED(inparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::remove(const ClusterRackInfo& inparam)
    {
      UNUSED(inparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::scan(VClusterRackInfo& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_cluster_rack_info");
        snprintf(sql, 1024, "select cluster_rack_id, cluster_id, ns_vip, "
            "cluster_stat, rem from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        ClusterRackInfo tmp;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          tmp.cluster_rack_id_ = atoi(row[0]);
          tmp.cluster_data_.cluster_id_ = row[1];
          tmp.cluster_data_.ns_vip_ = row[2];
          tmp.cluster_data_.cluster_stat_ = atoi(row[3]);
          tmp.cluster_data_.access_type_ = -1;
          snprintf(tmp.rem_, REM_LEN, "%s", row[4]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }
    int MysqlDatabaseHelper::scan(VClusterRackGroup& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_cluster_rack_group");
        snprintf(sql, 1024, "select cluster_group_id, cluster_rack_id, cluster_rack_access_type, "
            "rem from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        ClusterRackGroup tmp;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          tmp.cluster_group_id_ = atoi(row[0]);
          tmp.cluster_rack_id_ = atoi(row[1]);
          tmp.cluster_rack_access_type_ = atoi(row[2]);
          snprintf(tmp.rem_, REM_LEN, "%s", row[3]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }
    int MysqlDatabaseHelper::scan(IpReplaceHelper::VIpTransferItem& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_caculate_ip_info");
        snprintf(sql, 1024, "select source_ip, caculate_ip from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          IpReplaceHelper::IpTransferItem item;
          int r = TFS_SUCCESS;
          r = item.set_source_ip(row[0]);
          if (TFS_SUCCESS != r)
          {
            TBSYS_LOG(WARN, "read a invalid source ip from t_caculate_ip_info ip is %s", row[0]);
            continue;
          }
          r = item.set_dest_ip(row[1]);
          if (TFS_SUCCESS != r)
          {
            TBSYS_LOG(WARN, "read a invalid dest ip from t_caculate_ip_info ip is %s", row[1]);
            continue;
          }
          outparam.push_back(item);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }
    int MysqlDatabaseHelper::scan(std::map<int32_t, IpReplaceHelper::VIpTransferItem>& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_app_ip_replace");
        snprintf(sql, 1024, "select app_id, source_ip, turn_ip from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          IpReplaceHelper::IpTransferItem item;
          int r = TFS_SUCCESS;
          r = item.set_source_ip(row[1]);
          if (TFS_SUCCESS != r)
          {
            TBSYS_LOG(WARN, "read a invalid source ip from t_app_ip_replace ip is %s", row[1]);
            continue;
          }
          r = item.set_dest_ip(row[2]);
          if (TFS_SUCCESS != r)
          {
            TBSYS_LOG(WARN, "read a invalid turn ip from t_app_ip_replace ip is %s", row[2]);
            continue;
          }
          int32_t app_id = atoi(row[0]);
          outparam[app_id].push_back(item);
        }
error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::scan(VClusterRackDuplicateServer& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_cluster_rack_duplicate_server");
        snprintf(sql, 1024, "select cluster_rack_id, dupliate_server_addr from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        ClusterRackDuplicateServer tmp;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          tmp.cluster_rack_id_ = atoi(row[0]);
          snprintf(tmp.dupliate_server_addr_, ADDR_INFO_LEN, "%s", row[1]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::select(BaseInfoUpdateTime& outparam)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_base_info_update_time");
        snprintf(sql, 1024, "select UNIX_TIMESTAMP(base_last_update_time), UNIX_TIMESTAMP(app_last_update_time) from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }
        ret = TFS_ERROR;

        while(NULL != (row = mysql_fetch_row(mysql_ret) ))
        {
          outparam.base_last_update_time_ = atoi(row[0]);
          outparam.base_last_update_time_ *= 1000 * 1000;
          outparam.app_last_update_time_ = atoi(row[1]);
          outparam.app_last_update_time_ *= 1000 * 1000;
          ret = TFS_SUCCESS;
          break;
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::select(const AppInfo& inparam, AppInfo& outparam)
    {
      UNUSED(inparam);
      UNUSED(outparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::update(const AppInfo& inparam)
    {
      UNUSED(inparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::remove(const AppInfo& inparam)
    {
      UNUSED(inparam);
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::scan(MIdAppInfo& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_app_info");
        snprintf(sql, 1024, "select app_key, id, quto, cluster_group_id, "
            "app_name, app_owner, report_interval, "
            "need_duplicate, rem, UNIX_TIMESTAMP(modify_time), use_remote_cache from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        AppInfo tmp;

        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          snprintf(tmp.key_, APP_KEY_LEN, "%s", row[0]);
          tmp.id_ = atoi(row[1]);
          tmp.quto_ = strtoll(row[2], NULL, 10);
          tmp.cluster_group_id_ = atoi(row[3]);
          snprintf(tmp.app_name_, REM_LEN, "%s", row[4]);
          snprintf(tmp.app_owner_, REM_LEN, "%s", row[5]);
          tmp.report_interval_ = atoi(row[6]);
          tmp.need_duplicate_ = atoi(row[7]);
          snprintf(tmp.rem_, REM_LEN, "%s", row[8]);
          tmp.modify_time_ = atoi(row[9]);
          tmp.use_remote_cache_ = atoi(row[10]);
          tmp.modify_time_ *= 1000 * 1000;

          outparam[tmp.id_] = tmp;
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::update_session_info(const std::vector<SessionBaseInfo>& session_infos)
    {
      int ret = TFS_SUCCESS;
      const int SQLS_IN_STR = 100;
      char* sql = new char[(1024 + 256) * SQLS_IN_STR];
      int session_size = session_infos.size();
      int done = 0;
      int pos = 0;
      static const char* S_NOW = "now()";
      static const char* s_NULL = "NULL";
      while (done < session_size)
      {
        pos = 0;
        for (int i = 0 ; i < SQLS_IN_STR && done < session_size; i++)
        {
          const SessionBaseInfo& session = session_infos[done];
          const char* log_out_time = s_NULL;
          if (session.is_logout_)
          {
            log_out_time = S_NOW;
          }
          pos += snprintf(sql + pos, 1024, "insert into t_session_info "
              "(session_id,cache_size,cache_time,client_version,log_out_time,create_time,modify_time) "
              "values ('%s',%"PRI64_PREFIX"d,%"PRI64_PREFIX"d,'%s',%s,now(),now())",
              session.session_id_.c_str(), session.cache_size_, session.cache_time_,
              session.client_version_.c_str(), log_out_time);
          if (session.is_logout_)
          {
            pos += snprintf(sql + pos, 256, " on duplicate key update log_out_time=now(),modify_time=now(),"
                "cache_size=%"PRI64_PREFIX"d, cache_time=%"PRI64_PREFIX"d,"
                "client_version='%s';",
                session.cache_size_, session.cache_time_, session.client_version_.c_str());
          }
          else
          {
            pos += snprintf(sql + pos, 256, " on duplicate key update modify_time=now(),"
                "cache_size=%"PRI64_PREFIX"d, cache_time=%"PRI64_PREFIX"d,"
                "client_version='%s';",
                session.cache_size_, session.cache_time_, session.client_version_.c_str());
          }
          done++;
        }
        ret = exect_update_sql(sql);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
      }
      delete []sql;
      return ret;
    }
    int MysqlDatabaseHelper::update_session_stat(const std::map<std::string, SessionStat>& session_stats)
    {
      int ret = TFS_SUCCESS;
      const int SQLS_IN_STR = 100;
      char* sql = new char[(1024 + 1024) * SQLS_IN_STR];
      std::map<std::string, SessionStat>::const_iterator it = session_stats.begin();
      int pos = 0;
      int done = 0;
      while (it != session_stats.end() && TFS_SUCCESS == ret)
      {
        std::map<OperType, AppOperInfo>::const_iterator inner_it = it->second.app_oper_info_.begin();
        for (; inner_it != it->second.app_oper_info_.end() && TFS_SUCCESS == ret; inner_it++)
        {

          pos += snprintf(sql + pos, 1024, "insert into t_session_stat "
              "(session_id,oper_type,oper_times,file_size,response_time,succ_times,create_time,modify_time) "
              "values ('%s',%d,%"PRI64_PREFIX"d,%"PRI64_PREFIX"d,%"PRI64_PREFIX"d,%"PRI64_PREFIX"d,now(),now())",
              it->first.c_str(), inner_it->first,
              inner_it->second.oper_times_, inner_it->second.oper_size_,
              inner_it->second.oper_rt_, inner_it->second.oper_succ_);

          pos += snprintf(sql + pos, 1024, " on duplicate key update "
              "oper_times=oper_times+%"PRI64_PREFIX"d,file_size=file_size+%"PRI64_PREFIX"d,"
              "response_time=response_time+%"PRI64_PREFIX"d,succ_times=succ_times+%"PRI64_PREFIX"d,"
              "modify_time=now();",
              inner_it->second.oper_times_, inner_it->second.oper_size_,
              inner_it->second.oper_rt_, inner_it->second.oper_succ_);
          done++;
          if (done >= SQLS_IN_STR)
          {
            done = 0;
            pos = 0;
            ret = exect_update_sql(sql);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
        }
        it++;
      }
      if (TFS_SUCCESS == ret && done > 0)
      {
        ret = exect_update_sql(sql);
      }
      delete []sql;
      return ret;
    }

    int MysqlDatabaseHelper::update_app_stat(const MIdAppStat& app_stats)
    {
      int ret = TFS_SUCCESS;
      const int SQLS_IN_STR = 100;
      char* sql = new char[(512 + 512 ) * SQLS_IN_STR];
      MIdAppStat::const_iterator it = app_stats.begin();
      int pos = 0;
      while (it != app_stats.end())
      {
        pos = 0;
        for (int i = 0 ; i < SQLS_IN_STR && it != app_stats.end(); i++)
        {
          pos += snprintf(sql + pos, 512, "insert into t_app_stat"
              "(app_id, used_capacity, file_count, create_time, modify_time) "
              "values (%d,%"PRI64_PREFIX"d,%"PRI64_PREFIX"d,now(),now())",
              it->first, it->second.used_capacity_, it->second.file_count_);
          pos += snprintf(sql + pos, 512, " on duplicate key update "
              "used_capacity=used_capacity+%"PRI64_PREFIX"d,file_count=file_count+%"PRI64_PREFIX"d, modify_time=now();",
              it->second.used_capacity_, it->second.file_count_);
          it++;
        }
        ret = exect_update_sql(sql);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
      }
      delete []sql;
      return ret;
    }

    int MysqlDatabaseHelper::scan_cache_info(std::vector<std::string>& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "t_cluster_cache_info");
        snprintf(sql, 1024, "select cache_server_addr from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret)
        {
          TBSYS_LOG(ERROR, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        std::string tmp;
        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL)
        {
          TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(NULL != (row = mysql_fetch_row(mysql_ret)))
        {
          tmp = row[0];
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::exect_update_sql(const char* sql)
    {
      int ret = TFS_ERROR;
      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        ret = mysql_query(&mysql_.mysql, sql);
        do
        {
          MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
          mysql_free_result(mysql_ret);
          if (ret)
          {
            TBSYS_LOG(ERROR, "error is %s sql is %s",  mysql_error(&mysql_.mysql), sql);
            close();
            ret = TFS_ERROR;
          }
        } while (!mysql_next_result(&mysql_.mysql));
      }
      return ret;
    }

  }
}
