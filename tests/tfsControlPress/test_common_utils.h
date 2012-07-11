/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_common_utils.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#ifndef TEST_COMMON_UTILS_H
#define TEST_COMMON_UTILS_H

#include "internal.h"
#include "tbsys.h"

using namespace tfs::common;
using namespace tbsys;

typedef std::vector<VUINT32*> VPTR_UINT32;
typedef std::vector<VSTRING*> VPTR_STR;
 
class TestCommonUtils {
  public:
    static int generateData(char *data, int size);
    static int readFilelist(char *filelist, VUINT32& crcSet, VSTRING& filenameSet);
    static int getFilelist(int partNo, int partSize, VUINT32& crcSet,
           VUINT32& crcSetPerThread, VSTRING& filenameSet, VSTRING& filenameSetPerThread);

};

#endif
