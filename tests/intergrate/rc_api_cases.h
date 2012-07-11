/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rc_api_cases.h 498 2011-06-14 08:53:47Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include <vector>
#include <map>
#include <tbsys.h>
#include <gtest/gtest.h>
#include "common/define.h"
#include "common/base_packet_streamer.h"
#include "message/message_factory.h"
#include "new_client/tfs_rc_helper.h"

class RcApiTest : public testing::Test 
{
  public:
    static void SetUpTestCase();
    static void TearDownTestCase();

  public:
    static void login(const int num);
    static void keep_alive(const int num);
    //static int write_new_file(const int32_t length) ;
    //static int write_read_file(const int32_t length) ;
    //static int write_update_file(const int32_t length) ;
    //static int unlink_file(const int32_t length);
    //static int rename_file(const int32_t length);

  private:
    //static int read_exist_file(const string& tfs_file_name, char* buffer, int32_t& length);
    //static int stat_exist_file(char* tfs_file_name, tfs::common::FileInfo& file_info);
    //static int write_new_file(const char* buffer, int32_t length) ;
    //static int write_update_file(const char* buffer, int32_t length, const string& tfs_file_name);
    //static int generate_data(char* buffer, const int32_t length);
    //static int write_data(const char* buffer, const int32_t length);
  public:
    static std::vector<std::string> authorized_keys_;
    static std::vector<std::string> not_authorized_keys_;
    static std::vector<uint64_t> app_ids_;
    static int64_t rc_ip_;

    static tfs::common::BasePacketFactory* packet_factory_;
    static tfs::common::BasePacketStreamer* packet_streamer_;

};
