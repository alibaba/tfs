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
#ifndef TFS_RCSERVER_IP_REPLACE_HELPER_H_
#define TFS_RCSERVER_IP_REPLACE_HELPER_H_
#include <string>
#include <vector>
#include "common/define.h"
namespace tfs
{
  namespace rcserver
  {
    class IpReplaceHelper
    {
      public:
        class IpTransferItem
        {
          public:
            IpTransferItem();
            void clear();
            int set_source_ip(const std::string& ip);
            int set_dest_ip(const std::string& ip);
            std::string get_source_ip() const;
            std::string get_dest_ip() const;

            int transfer(IpTransferItem& in) const;

          private:
            int set_ip(int* d, const std::string& ip);
            std::string get_ip(const int* d) const;
            int source_ip_[4];
            int dest_ip_[4];
        };
        typedef std::vector<IpTransferItem> VIpTransferItem;
        static int replace_ip(const VIpTransferItem& transfer_item,
            const std::string& source_ip, std::string& dest_ip);
        static uint32_t calculate_distance(const std::string& ip_str, const std::string& addr);
    };
  }
}
#endif
