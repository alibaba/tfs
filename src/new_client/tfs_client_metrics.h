/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_metrics.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_METRICS_H_
#define TFS_CLIENT_METRICS_H_
#include "common/internal.h"

namespace tfs
{
  namespace client
  {
#ifdef __CLIENT_METRICS__
  class ClientMetrics
  {
  public:
    ClientMetrics():
    total_count_(0),
    timeout_count_(0),
    failed_count_(0),
    response_consume_(0),
    max_response_(0)
    {

    }
    virtual ~ClientMetrics()
    {}

    inline void reset()
    {
      total_count_ = timeout_count_ = failed_count_ = response_consume_ = max_response_ = 0;
    }

    inline void set_response_time(const uint64_t response)
    {
      response_consume_ += response;
      if (max_response_ < response)
      {
        max_response_ = response;
      }
    }
    inline void print(const char* head, const uint32_t interval)
    {
      if (total_count_ >= interval)
      {
        TBSYS_LOG(DEBUG, "%s total(%"PRI64_PREFIX"u), timeout(%"PRI64_PREFIX"u), failed(%"PRI64_PREFIX"u), "
            "average response(%5.2f)ms, max response:(%5.2f)ms",
            head, total_count_, timeout_count_, failed_count_,
            (double) response_consume_ / (interval * 1000),
            (double)max_response_ / 1000);
        reset();
      }
    }
    inline void incr_total_count()
    {
      tbutil::Mutex::Lock lock(mutex_);
      ++total_count_;
    }

    inline void incr_failed_count()
    {
      tbutil::Mutex::Lock lock(mutex_);
      ++failed_count_;
    }
    inline void incr_timeout_count()
    {
      tbutil::Mutex::Lock lock(mutex_);
      ++timeout_count_;
    }
    inline void stat(const char* head, const uint64_t response, const uint32_t count, const uint32_t interval)
    {
      tbutil::Mutex::Lock lock(mutex_);
      total_count_ += count;
      set_response_time(response);
      print(head, interval);
    }
  private:
    tbutil::Mutex mutex_;
    DISALLOW_COPY_AND_ASSIGN(ClientMetrics);
    uint64_t total_count_;
    uint64_t timeout_count_;
    uint64_t failed_count_;
    uint64_t response_consume_;
    uint64_t max_response_;
  };
#endif
  }
}
#endif
