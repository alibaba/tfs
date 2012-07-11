/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_large_file_cases.h 166 2011-03-15 07:35:48Z zongdai@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TEST_IOAPI_TEST_H_
#define TFS_TEST_IOAPI_TEST_H_
#include <gtest/gtest.h>
#include "tfs_ioapi_util.h"

class TfsLargeFileTest : public testing::Test
{
  public:
    static void SetUpTestCase();
    static void TearDownTestCase();

  public:
    static void test_read(const bool large_flag, int64_t base, const char* suffix = NULL);
    static void unlink_process(const bool large_flag);
    static void test_update(const bool large_flag, int64_t base, const char* suffix = NULL);
};
#endif
