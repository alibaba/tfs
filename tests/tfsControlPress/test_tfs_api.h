/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_api.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#ifndef TEST_TFS_API_H
#define TEST_TFS_API_H

#include "tbsys.h"
#include "tbnet.h"

class TestTfsAPI : public tbsys::CDefaultRunnable {
  public:
    TestTfsAPI():CDefaultRunnable() {};
    /*virtual*/ ~TestTfsAPI() {};
    
    bool init(const char *filename);
    void setTestIndex(const char *testIndex);
    //Runnable
    void run(tbsys::CThread *thread, void *arg);
    void wait() {
  	  tbsys::CDefaultRunnable::wait();
    }
   	int64_t _start_time;
    

  private:

    char *_testIndex;
    int _testThreadCount;
    int64_t _timeInter;
};

#endif
