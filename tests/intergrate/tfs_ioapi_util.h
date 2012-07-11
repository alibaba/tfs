/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_ioapi_util.h 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_TEST_IOAPI_UTIL_H_
#define TFS_TEST_IOAPI_UTIL_H_
#include <tbsys.h>
#include <gtest/gtest.h>
#include "message/message_factory.h"
#include "common/define.h"
#ifdef USE_CPP_CLIENT
#include "new_client/tfs_client_api.h"
#else
#include "new_client/tfs_client_capi.h"
#endif

class TfsIoApiUtil
{
  public:
    TfsIoApiUtil();
    ~TfsIoApiUtil();

  public:
    static int64_t write_new_file(const bool large_flag, const int64_t length, uint32_t& crc,
                   char* tfsname = NULL, const char* suffix = NULL, const int32_t name_len = 0);
    static int read_exist_file(const bool large_flag, const char* tfs_name, const char* suffix, uint32_t& read_crc);
  static int unlink_file(const char* tfsname, const char* suffix = NULL, const tfs::common::TfsUnlinkType action = tfs::common::DELETE);
    static int stat_exist_file(const bool large_flag, char* tfsname, tfs::common::TfsFileStat& file_info);
    static int generate_data(char* buffer, const int32_t length);
    static int generate_length(int64_t& length, int64_t base);

  private:
    //static const int64_t PER_SIZE = 8 * 1024 * 1024;
    static const int64_t PER_SIZE = 19 * 512 * 1024;
    //static const int64_t SEG_SIZE = 1024 * 1024;
    static const int64_t SEG_SIZE = PER_SIZE;

  public:
#ifdef USE_CPP_CLIENT
    static tfs::client::TfsClient* tfs_client_;
#endif
};
#endif
