/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_statis.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_tfs_statis.h"

void TestTfsStatis::addStatis(int64_t start_time, int64_t end_time, int ret,
  const char *testCase  /*=Statis*/)
{
  TestTfsStatis::TimeInterval timeInter;
  timeInter._start_time = start_time;
  timeInter._end_time = end_time;
  _mutex.lock();
  timeInter._count = _total_count ++;
  bool sucess = (ret == 0) ? true : false;
  timeInter._sucess = sucess;
  if(sucess){
    ++_sucess_count;
    ++_interval_success;
  }else{
    ++_failed_count;
    ++_interval_failed;
  }
  if (_stdd_time == 0)
  {
    _stdd_time = end_time;
    _fTps = 0;
    _interval_success = -1;
  }
  float fTps = 0;
  if ((end_time - _stdd_time)  / 1000000.0f != 0)
  {
    fTps = (float)_interval_success / ((end_time - _stdd_time) / 1000000.0f);
  }
  if (_total_count > 1)
  {
    _fTps = (fTps - _fTps) / (_total_count - 1 ) + _fTps;
  }

#if 0
  TBSYS_LOG(ERROR,"%s:TPS(T/s):%f",testCase, (float)fTps);
#endif
  //char strTime[16];
  //TBSYS_LOG(ERROR,"start:%s,end:%s : %s",CTimeUtil::timeToStr(start_time,strTime),CTimeUtil::timeToStr(end_time,strTime),sucess ? "SUCCESS" : "FAILED");
  if (_total_count % 1000 == 0) {
    //TBSYS_LOG(ERROR,"starttime=%lld, endtime=%lld", _stdd_time, end_time);
    //TBSYS_LOG(ERROR,"%s:%d:SUCCESS:%d,FAILED:%d:TPS(T/s):%f",testCase,_total_count/100,_interval_success,_interval_failed, _interval_success / (end_time /1000000 - _stdd_time / 1000000) );
    TBSYS_LOG(INFO,"%s : %d: SUCCESS:%d, FAILED:%d, TPS(T/s):%f, sum_success:%d, sum_failed:%d interval_time:%f",
      testCase,_total_count/1000,_interval_success,_interval_failed,
      (float)fTps, _sucess_count, _failed_count,
      ( (float)end_time / 1000000 - (float)_stdd_time / 1000000) );
    _interval_success = 0;
    _interval_failed = 0;
    _stdd_time = end_time;
  }
  //_time_statis.push_back(timeInter);
  _mutex.unlock();
  //TBSYS_LOG(ERROR,"total:%d,failed:%d,success:%d",_total_count,_failed_count,_sucess_count);
}

void TestTfsStatis::displayStatis(const char *testCase)
{
  TBSYS_LOG(INFO, "%s: total:%d, failed:%d, success:%d, successful rate:%f%, TPS:%f",
    testCase, _total_count, _failed_count, _sucess_count,
    (float)_sucess_count / (float)_total_count * 100, _fTps);
  // std::vector<TimeInterval>::iterator it = _time_statis.begin();
  // char strTime[16];
  // for(;it != _time_statis.end();it++){
  //	TBSYS_LOG(ERROR,"%d : start:%s,end:%s : %s",it->_count,CTimeUtil::timeToStr(it->_start_time,strTime),CTimeUtil::timeToStr(it->_end_time,strTime),it->_sucess ? "SUCCESS" : "FAILED");
  //   }
}

void TestTfsStatis::reset()
{
  _sucess_count = 0;
  _interval_success = 0;
  _failed_count = 0;
  _interval_failed = 0;
  _total_count = 0;
  _stdd_time = 0;
}
