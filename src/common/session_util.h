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
 *          zongdai(zongdai@taobao.com)
 *
 */
#ifndef TFS_COMMON_SESSIONUTIL_H_
#define TFS_COMMON_SESSIONUTIL_H_

#include <string>

namespace tfs
{
  namespace common
  {
    static const char SEPARATOR_KEY = '-';
    class SessionUtil
    {
      public:
        static std::string gene_uuid_str();
        static void gene_session_id(const int32_t app_id, const int64_t session_ip, std::string& session_id);
        static int parse_session_id(const std::string& session_id, int32_t& app_id, int64_t& session_ip);
    };
  }
}
#endif //TFS_RCSERVER_SESSIONUUID_H_
