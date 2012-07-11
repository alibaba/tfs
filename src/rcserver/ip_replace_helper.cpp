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
#include "ip_replace_helper.h"

#include "common/func.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace std;
    IpReplaceHelper::IpTransferItem::IpTransferItem()
    {
      clear();
    }
    void IpReplaceHelper::IpTransferItem::clear()
    {
      for (int i = 0; i < 4; i++)
      {
        source_ip_[i] = -1;
        dest_ip_[i] = -1;
      }
    }
    int IpReplaceHelper::IpTransferItem::set_source_ip(const std::string& ip)
    {
      return set_ip(source_ip_, ip);
    }

    int IpReplaceHelper::IpTransferItem::set_dest_ip(const std::string& ip)
    {
      return set_ip(dest_ip_, ip);
    }
    std::string IpReplaceHelper::IpTransferItem::get_source_ip() const
    {
      return get_ip(source_ip_);
    }
    std::string IpReplaceHelper::IpTransferItem::get_dest_ip() const
    {
      return get_ip(dest_ip_);
    }
    int IpReplaceHelper::IpTransferItem::set_ip(int* d, const std::string& ip)
    {
      int ret = common::TFS_SUCCESS;
      vector<string> fields;
      tfs::common::Func::split_string(ip.c_str(), '.', fields);
      if (fields.size() != 4)
      {
        TBSYS_LOG(ERROR, "%s is not a valiable input parameter", ip.c_str());
        ret = common::TFS_ERROR;
      }
      for (int i = 0; i < 4; i++)
      {
        if ("*" == fields[i])
        {
          d[i] = -1;
        }
        else
        {
          d[i] = atoi(fields[i].c_str());
          if (0 == d[i] && "0" != fields[i])
          {
            ret = common::TFS_ERROR;
            break;
          }
        }
        //TBSYS_LOG(DEBUG, "%d %d", i, d[i]);
      }
      return ret;
    }
    std::string IpReplaceHelper::IpTransferItem::get_ip(const int* d) const
    {
      char tmp[64];
      tmp[0] = 0;
      if (d[0] >= 0 && d[1] >= 0 &&
          d[2] >= 0 && d[3] >= 0)
      {
        snprintf(tmp, 64, "%d.%d.%d.%d", d[0], d[1], d[2], d[3]);
      }
      return tmp;
    }
    int IpReplaceHelper::IpTransferItem::transfer(IpTransferItem& in) const
    {
      int match_count = -1;
      int i = 0;
      for (; i < 4; i++)
      {
        if (in.source_ip_[i] != source_ip_[i])
        {
          if (-1 != source_ip_[i])
          {
            //not match
            match_count = -1;
            break;
          }
          else
          {
            if (-1 == match_count) match_count = i;
            in.dest_ip_[i] = in.source_ip_[i];
          }
        }
        else
        {
          if (dest_ip_[i] < 0)
          {
            in.dest_ip_[i] = in.source_ip_[i];
          }
          else
          {
            in.dest_ip_[i] = dest_ip_[i];
          }
        }
      }
      if (4 == i && -1 == match_count) match_count = 4;

      return match_count;
    }
    int IpReplaceHelper::replace_ip(const VIpTransferItem& transfer_item,
        const std::string& source_ip, std::string& dest_ip)
    {
      int match_count = 0;
      int ret = common::TFS_SUCCESS;
      IpTransferItem item;
      IpTransferItem result_item;
      int result_match_count = -1;
      ret = item.set_source_ip(source_ip);
      if (common::TFS_SUCCESS == ret)
      {
        VIpTransferItem::const_iterator it = transfer_item.begin();
        for (; it != transfer_item.end(); it++)
        {
          match_count = it->transfer(item);
          if (match_count > result_match_count)
          {
            result_match_count = match_count;
            result_item = item;
          }
        }
        if (result_match_count < 0)
        {
          ret = common::TFS_ERROR;
        }
        else
        {
          dest_ip = result_item.get_dest_ip();
        }
      }
      return ret;
    }
    uint32_t IpReplaceHelper::calculate_distance(const std::string& ip_str, const std::string& addr)
    {
      uint32_t ip1 = common::Func::get_addr(ip_str.c_str());
      uint32_t ip2 = common::Func::get_addr(addr.c_str());
      uint32_t mask = 0xff;
      uint32_t n1 = 0;
      uint32_t n2 = 0;
      for (int i = 0; i < 4; i++)
      {
        n1 <<=  8;
        n2 <<= 8;
        n1 |= ip1 & mask;
        n2 |= ip2 & mask;
        ip1 >>= 8;
        ip2 >>= 8;
      }
      uint32_t result = 0;
      if (n1 > n2)
      {
        result = n1 - n2;
      }
      else
      {
        result = n2 - n1;
      }
      return result;
    }
  }
}
