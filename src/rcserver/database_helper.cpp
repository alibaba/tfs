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

#include "database_helper.h"
#include "common/internal.h"
namespace tfs
{
  using namespace common;
  namespace rcserver
  {
    DatabaseHelper::DatabaseHelper()
    {
      conn_str_[0] = '\0';
      user_name_[0] = '\0';
      passwd_[0] = '\0';
      is_connected_ = false;
    }

    DatabaseHelper::~DatabaseHelper()
    {
    }

    int DatabaseHelper::set_conn_param(const char* conn_str, const char* user_name, const char* passwd)
    {
      int ret = TFS_SUCCESS;
      if (NULL == conn_str || NULL == user_name || NULL == passwd)
      {
        ret = TFS_ERROR;
      }
      else
      {
        snprintf(conn_str_, CONN_STR_LEN, "%s", conn_str);
        snprintf(user_name_, CONN_STR_LEN, "%s", user_name);
        snprintf(passwd_, CONN_STR_LEN, "%s", passwd);
      }
      return ret;
    }

    bool DatabaseHelper::is_connected() const
    {
      return is_connected_;
    }

  }
}
