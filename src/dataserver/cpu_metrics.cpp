/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: cpu_metrics.cpp 187 2011-04-07 06:35:21Z duanfei@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <stdlib.h>
#include <string.h>
#include "cpu_metrics.h"
#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;

    CpuMetrics::CpuMetrics()
    {
      // get info from /proc/stat
      for (uint32_t i = 0; i < MAX_CPU_COUNT; ++i)
        memset(&cpu_data_[i], 0, sizeof(CpuInfo));
      fp_ = NULL;
      refresh();
    }

    CpuMetrics::~CpuMetrics()
    {
      if (fp_ != NULL)
        fclose(fp_);
      fp_ = NULL;
    }

    int CpuMetrics::refresh()
    {
      if (!fp_)
      {
        fp_ = fopen("/proc/stat", "r");
        if (!fp_)
        {
          valided_ = false;
          return TFS_ERROR;
        }
      }
      rewind(fp_);
      fflush(fp_);

      cpu_count_ = 0;

      char* line = NULL;
      size_t len = 0;
      ssize_t read;
      int32_t num = 0;
      while ((read = getline(&line, &len, fp_)) != -1)
      {
        if (strstr(line, "cpu"))
        {
          if (cpu_count_ >= MAX_CPU_COUNT)
            break;
          if (cpu_count_ == 0)
          {
            num
                = sscanf(
                    line,
                    "cpu %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u",
                    &cpu_data_[cpu_count_].u, &cpu_data_[cpu_count_].n, &cpu_data_[cpu_count_].s,
                    &cpu_data_[cpu_count_].i, &cpu_data_[cpu_count_].w, &cpu_data_[cpu_count_].x,
                    &cpu_data_[cpu_count_].y, &cpu_data_[cpu_count_].z);
            if (num < 4)
            {
              continue;
            }
            cpu_data_[cpu_count_].id = -1; // all
          }
          else
          {
            num
                = sscanf(
                    line,
                    "cpu%u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u %" PRI64_PREFIX "u",
                    &cpu_data_[cpu_count_].id, &cpu_data_[cpu_count_].u, &cpu_data_[cpu_count_].n,
                    &cpu_data_[cpu_count_].s, &cpu_data_[cpu_count_].i, &cpu_data_[cpu_count_].w,
                    &cpu_data_[cpu_count_].x, &cpu_data_[cpu_count_].y, &cpu_data_[cpu_count_].z);
            if (num < 4)
            {
              continue;
            }
          }
          ++cpu_count_;
        }
      }

      if (line)
        ::free(line);

      valided_ = (cpu_count_ > 0);

      return cpu_count_;
    }

    int CpuMetrics::dump(const uint32_t index)
    {
      if (index >= cpu_count_)
        return TFS_ERROR;
      const CpuInfo& c = cpu_data_[index];
      fprintf(
          stdout,
          "now: u:%" PRI64_PREFIX "u s:%" PRI64_PREFIX "u n:%" PRI64_PREFIX "u i:%" PRI64_PREFIX "u w:%" PRI64_PREFIX "u x:%" PRI64_PREFIX "u y:%" PRI64_PREFIX "u z:%" PRI64_PREFIX "u\n",
          c.u, c.s, c.n, c.i, c.w, c.x, c.y, c.z);
      fprintf(
          stdout,
          "last: u:%" PRI64_PREFIX "u s:%" PRI64_PREFIX "u n:%" PRI64_PREFIX "u i:%" PRI64_PREFIX "u w:%" PRI64_PREFIX "u x:%" PRI64_PREFIX "u y:%" PRI64_PREFIX "u z:%" PRI64_PREFIX "u\n",
          c.u_sav, c.s_sav, c.n_sav, c.i_sav, c.w_sav, c.x_sav, c.y_sav, c.z_sav);
      return TFS_SUCCESS;
    }

    int CpuMetrics::save(const uint32_t index)
    {
      if (index >= cpu_count_)
        return TFS_ERROR;
#define save_m(index, a)  do { cpu_data_[index].a##_sav = cpu_data_[index].a;  } while (0);
      save_m(index, u);
      save_m(index, s);
      save_m(index, n);
      save_m(index, i);
      save_m(index, w);
      save_m(index, x);
      save_m(index, y);
      save_m(index, z);
      return TFS_SUCCESS;
    }

    int CpuMetrics::calc(const uint32_t index, UsageInfo& ret)
    {
#define TRIMz(x) ((tz = (SIC_t)(x)) < 0 ? 0 : tz)
#define calc_frme(index, a)  do { a##_frme = cpu_data_[index].a - cpu_data_[index].a##_sav; } while(0);
#define calc_trim_frme(index, a)  do { a##_frme = TRIMz(cpu_data_[index].a - cpu_data_[index].a##_sav); } while(0);
#define calc_usage(x, a) do {x.a = (float)a##_frme * scale; } while(0);
      if (index >= cpu_count_)
        return TFS_ERROR;
      SIC_t u_frme, s_frme, n_frme, i_frme, w_frme, x_frme, y_frme, z_frme, tot_frme, tz;
      float scale;

      calc_frme(index, u);
      calc_frme(index, s);
      calc_frme(index, n);
      calc_trim_frme(index, i);
      calc_frme(index, w);
      calc_frme(index, x);
      calc_frme(index, y);
      calc_frme(index, z);

      tot_frme = u_frme + s_frme + n_frme + i_frme + w_frme + x_frme + y_frme + z_frme;
      if (tot_frme < 1)
        tot_frme = 1;
      scale = 100.0 / (float) tot_frme;

      calc_usage(ret, u);
      calc_usage(ret, s);
      calc_usage(ret, n);
      calc_usage(ret, i);
      calc_usage(ret, w);
      calc_usage(ret, x);
      calc_usage(ret, y);
      calc_usage(ret, z);

      return TFS_SUCCESS;
    }

    int CpuMetrics::saveall()
    {
      for (uint32_t i = 0; i < cpu_count_; ++i)
      {
        save(i);
      }
      return TFS_SUCCESS;
    }

    int CpuMetrics::calcall()
    {
      for (uint32_t i = 0; i < cpu_count_; ++i)
      {
        calc(i, usage_data_[i]);
      }
      return TFS_SUCCESS;
    }

    int CpuMetrics::summary()
    {
      refresh();
      calcall();
      saveall();
      return TFS_SUCCESS;
    }

  }
}
