/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: adminserver.cpp 18 2010-10-12 09:45:55Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#ifndef TFS_MESSAGE_ADMINCMDMESSAGE_H_
#define TFS_MESSAGE_ADMINCMDMESSAGE_H_

#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    enum AdminCmd
    {
      ADMIN_CMD_NONE = 0,
      ADMIN_CMD_CHECK,
      ADMIN_CMD_GET_STATUS,
      ADMIN_CMD_START_MONITOR,
      ADMIN_CMD_RESTART_MONITOR,
      ADMIN_CMD_STOP_MONITOR,
      ADMIN_CMD_START_INDEX,
      ADMIN_CMD_RESTART_INDEX,
      ADMIN_CMD_STOP_INDEX,
      ADMIN_CMD_KILL_ADMINSERVER,
      ADMIN_CMD_RESP
    };

    const int32_t ADMIN_MAX_INDEX_LENGTH = 127;
    struct MonitorStatus
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      char index_[ADMIN_MAX_INDEX_LENGTH+1];
      int32_t restarting_;
      int32_t failure_;
      int32_t pid_;
      int32_t dead_count_;
      int32_t start_time_;
      int32_t dead_time_;
      MonitorStatus()
      {
        memset(this, 0, sizeof(MonitorStatus));
      }

      MonitorStatus(const std::string& index)
      {
        memset(this, 0, sizeof(MonitorStatus));
        strncpy(index_, index.c_str(), ADMIN_MAX_INDEX_LENGTH);
      }

      inline std::string convert_time(int32_t time)
      {
        return time ? common::Func::time_to_str(time, 0) : "NON";
      }

      inline void dump()
      {
        bool warn = (0 == pid_) || (dead_count_ > common::ADMIN_WARN_DEAD_COUNT);
        fprintf(stderr, "%s%7s%7d%7d%7d%8d%23s%23s%s\n", warn ? "\033[31m" : "",
                index_, pid_, restarting_, failure_, dead_count_,
                convert_time(start_time_).c_str(), convert_time(dead_time_).c_str(),
                warn ? "\033[0m" : "");
      }
    };

    class AdminCmdMessage : public common::BasePacket
    {
    public:
      AdminCmdMessage();
      AdminCmdMessage(int32_t cmd_type);
      virtual ~AdminCmdMessage();
      virtual int serialize(common::Stream& output) const;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;

      inline void set_cmd_type(int32_t type)
      {
        type_ = type;
      }
      inline int32_t get_cmd_type()
      {
        return type_;
      }
      inline void set_index(const std::string& index)
      {
        index_.push_back(index);
      }
      inline void set_index(const common::VSTRING* index)
      {
        if (NULL != index)
          index_ = *index;
      }
      inline common::VSTRING* get_index()
      {
        return &index_;
      }
      inline std::vector<MonitorStatus>* get_status()
      {
        return &monitor_status_;
      }
      inline void set_status(MonitorStatus* monitor_status)
      {
        if (NULL != monitor_status)
          monitor_status_.push_back(*monitor_status);
      }
      inline void set_status(std::vector<MonitorStatus>* monitor_status)
      {
        if (NULL != monitor_status)
          monitor_status_ = *monitor_status;
      }
    private:
      int32_t type_;
      common::VSTRING index_;
      std::vector<MonitorStatus> monitor_status_;
    };
  }
}
#endif  // TFS_MESSAGE_ADMINCMDMESSAGE_H_
