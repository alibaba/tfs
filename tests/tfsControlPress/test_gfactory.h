/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_case_factory.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#ifndef TEST_GFACTORY_H
#define TEST_GFACTORY_H

#include <vector>
#include "test_tfs_api.h"
#include "test_tfs_statis.h"

class TestTfsStatis;
class TestTfsAPI;

class TestGFactory {

  public:
    static TestTfsStatis _statisForRead;
    static TestTfsStatis _statisForWrite;
    static TestTfsStatis _statisForUni;
    static TestTfsStatis _statisForUnlink;
    static TestTfsStatis _statisForUpdate;
    static TestTfsStatis _statisForUnique;
    static TestTfsStatis _statisForUniUnlink;
   
    static int _threadNo;
    static int _threadCount;
    static int _writeFileSize;
    static int _writeDelayTime;
    static int _index;
    static int64_t _batchCount;
    static int64_t _segSize;
    
    static TestTfsAPI _tfsTest;
};

#endif
