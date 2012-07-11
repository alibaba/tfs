/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_case.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#ifndef TEST_TFS_CASE_H
#define TEST_TFS_CASE_H

#include "tbnet.h"
#include "tbsys.h"
#include "tfs_file.h"
#include "dataserver_define.h"
#include "test_common_utils.h"

#define MAX_READ_SIZE 1048576
#define READ_MODE 1
#define WRITE_MODE 2
#define APPEND_MODE 4
#define UNLINK_MODE 8
#define NEWBLK_MODE 16
#define NOLEASE_MODE 32
#define THREAD_COUNT_MAX 300

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::dataserver;
using namespace tbsys;


class TestTfsCase {
  public:
    TestTfsCase():_totalSize(0),_currentFile(0){};
    virtual ~TestTfsCase() {};
    virtual int setUp();
    virtual void tearDown();
    virtual int run();
    virtual int tfsAssert(const char *msg, bool expect, int result);
    virtual int tfsAssertFail(const char *msg, int result);
  protected:
  
    TfsClient* _tfsFile;
    FileInfo _info;
    int64_t _totalSize;
    int _currentFile;
    int _largeFlag;
    int _lockFlag;
    int _loopFlag;

};

#endif
