/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_update.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#ifndef __TEST_TFS_UPDATE_H
#define __TEST_TFS_UPDATE_H

#include <stdio.h>
#include <set>
#include <string>
#include "test_tfs_case.h"
#include "dataserver_define.h"

#define TFSNAMELEN 19
#define LOCALKEYLEN 64
#define CRCSIZE 12


class TestTfsUpdate : public TestTfsCase {
  public:
    TestTfsUpdate():_data(NULL) {};
    ~TestTfsUpdate() {};
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
    //int _writeVerifyFlag;
    int _unlinkRatio;
      
    int writeData(char *data,int size);
    uint32_t _crc;
    char *_data;
    const char *_tfsFilename;
    std::set<std::string> _recordSet; // a record = tfs_filename + suffix + crc 
    static VUINT32 _crcSet;
    VUINT32 _crcSetPerThread; /* hold the crcSet per thread */
    static VSTRING _filenameSet;
    VSTRING _filenameSetPerThread; /* hold the filenameSet per thread */
    int _threadNo; /* [0.._threadCount-1] used to partition the filenameSet and crcSet*/
    static int _partSize;  /* partition size */
    static int _count;
    static bool _hasReadFilelist;
    bool _alreadyUnlinked;

    static tbsys::CThreadMutex _lock;
    static int _have_display_statis;
    
};
#endif
