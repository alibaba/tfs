/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_rc_helper.h 529 2011-06-20 09:29:28Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_RCHELPER_H_
#define TFS_CLIENT_RCHELPER_H_

#include <string>
#include "common/rc_define.h"

namespace tfs
{
  namespace client
  {
    class RcHelper
    {
    public:
      static int login(const uint64_t rc_ip, const std::string& app_key, const uint64_t app_ip,
          std::string& session_id, common::BaseInfo& base_info);
      static int keep_alive(const uint64_t rc_ip, const common::KeepAliveInfo& ka_info,
          bool& update_flag, common::BaseInfo& base_info);
      static int logout(const uint64_t rc_ip, const common::KeepAliveInfo& ka_info);
      static int reload(const uint64_t rc_ip, const common::ReloadType reload_type);

    private:
    };
  }
}

#endif  // TFS_CLIENT_RCHELPER_H_
