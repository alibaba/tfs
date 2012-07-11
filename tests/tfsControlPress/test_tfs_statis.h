/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_statis.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#ifndef TEST_TFS_STATIS_H
#define TEST_TFS_STATIS_H

#include <vector>
#include "tbsys.h"

class TestTfsStatis {
  public:
    TestTfsStatis():_total_count(0),_failed_count(0),_sucess_count(0),_interval_success(0),_interval_failed(0) {};
    ~TestTfsStatis() {};
    struct TimeInterval {
        int _count;
        bool _sucess;
        int64_t _start_time;
        int64_t _end_time;
    };

    void addStatis(int64_t start_time, int64_t end_time, int ret, const char *testCase = "Statis");
    void displayStatis(const char *testCase);
    void reset();

  private:
    volatile float _fTps;
    volatile int64_t _stdd_time;
    volatile int _total_count;
    volatile int _failed_count;
    volatile int _sucess_count;
    volatile int _interval_success;
    volatile int _interval_failed;
    std::vector<TimeInterval>  _time_statis;
    tbsys::CThreadMutex _mutex;
};
#endif
