/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_gfactory.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#include "test_gfactory.h"

TestTfsStatis TestGFactory::_statisForRead;
TestTfsStatis TestGFactory::_statisForWrite;
TestTfsStatis TestGFactory::_statisForUni;
TestTfsStatis TestGFactory::_statisForUnlink;
TestTfsStatis TestGFactory::_statisForUpdate;
TestTfsStatis TestGFactory::_statisForUnique;
TestTfsStatis TestGFactory::_statisForUniUnlink;
   
int TestGFactory::_threadNo = 0;
int TestGFactory::_threadCount = 0;
int TestGFactory::_writeFileSize = -1;
int TestGFactory::_writeDelayTime = 0;
int TestGFactory::_index = 0;
int64_t TestGFactory::_batchCount = 0;
int64_t TestGFactory::_segSize = 0;
    
TestTfsAPI TestGFactory::_tfsTest;


