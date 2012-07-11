/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_ioapi_cases.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <gtest/gtest.h>
#include "common/define.h"
#include "client/tfs_file.h"

#define EXIT_FILENAME_MATCH_ERROR1 -100
#define EXIT_FILENAME_MATCH_ERROR2 -101
#define EXIT_FILECONTENT_MATCHERROR -102


class TfsIoApiTest : public testing::Test 
{
  public:
    static void SetUpTestCase() ;
    static void TearDownTestCase() ;
  public:
    static int write_unique_file(const int32_t length);
    static int write_new_file(const int32_t length) ;
    static int write_read_file(const int32_t length) ;
    static int write_update_file(const int32_t length) ;
    static int unlink_file(const int32_t length);
    static int rename_file(const int32_t length);
    static const char* get_error_message()
    { 
      return tfs_client_->get_error_message();
    }

  private:
    static int read_exist_file(const string& tfs_file_name, char* buffer, int32_t& length);
    static int stat_exist_file(char* tfs_file_name, tfs::common::FileInfo& file_info);
    static int write_new_file(const char* buffer, int32_t length) ;
    static int write_update_file(const char* buffer, int32_t length, const string& tfs_file_name);
    static int generate_data(char* buffer, const int32_t length);
    static int write_data(const char* buffer, const int32_t length);
  public:
    static tfs::client::TfsClient* tfs_client_;
    static tfs::client::TfsFile tfsfile_;
    static tfs::client::TfsSession tfssession_;
};
