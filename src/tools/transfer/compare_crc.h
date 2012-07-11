/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compare_crc.h 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_COMPARE_CRC_H_
#define TFS_TOOLS_COMPARE_CRC_H_

#include <string>
#include <vector>
#include <libgen.h>
#include "common/internal.h"
#include "common/statistics.h"
#include "common/directory_op.h"
#include "new_client/tfs_session.h"
#include "new_client/tfs_file.h"
#include "new_client/fsname.h"
#include "common/client_manager.h"
#include "common/new_client.h"

struct StatStruct
{
  int64_t total_count_;
  int64_t succ_count_;
  int64_t fail_count_;
  int64_t error_count_;
  int64_t unsync_count_;
  int64_t skip_count_;
};

struct log_file
{
  FILE** fp_;
  const char* file_;
};

int get_crc_from_tfsname_list(const std::string& old_tfs_client, const std::string& new_tfs_client,   const std::string& modify_time, int64_t index, int64_t len);
int get_crc_from_block_list(const std::string& old_tfs_client, const std::string& new_tfs_client,
    const std::string& modify_time, int64_t index, int64_t len);

static StatStruct cmp_stat_;

#endif
