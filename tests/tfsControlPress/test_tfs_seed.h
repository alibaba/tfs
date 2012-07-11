/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_seed.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#ifndef __TEST_TFS_SEED_H
#define __TEST_TFS_SEED_H

#include <stdio.h>
#include <set>
#include <string>
#include "test_tfs_case.h"
#include "dataserver_define.h"

#define TFSNAMELEN 19
#define LOCALKEYLEN 64
#define CRCSIZE 12


class TestTfsSeed : public TestTfsCase {
  public:
    TestTfsSeed():_data(NULL) {};
    ~TestTfsSeed() {};
    int setUp();
    void tearDown();
    int run();
    int testWrite();
    int testRead();
    int testUnlink();
    int saveFilename();
  private:
    int _fd;
    int _keyIndex;
    int _writeCount;
    int _unit;
    int _unitMin;
    int _unitMax;
    char _localKey [ LOCALKEYLEN ];
    int _writeVerifyFlag;
    int _unlinkRatio;
      
    int retryOpen(int retry_count);
    int writeData(char *data,int size);
    uint32_t _preCrc;
    char *_data;
    char _fileName[ TFSNAMELEN ];
    std::set<std::string> _recordSet; // a record = tfs_filename + suffix + crc

    static tbsys::CThreadMutex _lock;
    static int _have_display_statis;
    bool _alreadyUnlinked;
};
#endif
