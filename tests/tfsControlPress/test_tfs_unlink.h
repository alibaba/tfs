/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_unlink.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#ifndef TEST_TFS_UNLINK_H
#define TEST_TFS_UNLINK_H

#include "test_tfs_case.h"
#include "dataserver_define.h"

class TestTfsUnlink : public TestTfsCase {
  public:

    TestTfsUnlink(){};
    ~TestTfsUnlink() {};
    int setUp();
    int testUnlink();
    int run();
    void tearDown();

  protected:
    static tbsys::CThreadMutex _lock;
    static int _have_display_statis;
    static VUINT32 _crcSet;
    VUINT32 _crcSetPerThread; /* hold the crcSet per thread */
    static VSTRING _filenameSet;
    VSTRING _filenameSetPerThread; /* hold the filenameSet per thread */
    int _threadNo; /* [0.._threadCount-1] used to partition the filenameSet and crcSet*/
    static int _partSize;  /* partition size */
    static int _count;
    static bool _hasReadFilelist;
};
#endif
