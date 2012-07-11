/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_case.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_tfs_case.h"
#include "test_gfactory.h"

#define NO_CLIENT_CACHE

using namespace tbsys;

CThreadMutex g_lock;

int TestTfsCase::setUp()
{
  g_lock.lock();
  _tfsFile = TfsClient::Instance();
#ifdef NO_CLIENT_CACHE
  _tfsFile->initialize( ( CConfig::getCConfig().getString("public", "ns_ip") ), 0, 1);
#else
  _tfsFile->initialize( ( CConfig::getCConfig().getString("public", "ns_ip") ) );
#endif
  TBSYS_LOG(DEBUG, "_batchCount = %d", TestGFactory::_batchCount);
  TBSYS_LOG(DEBUG, "_segSize = %d", TestGFactory::_segSize);
  if (TestGFactory::_batchCount != 0)
  { 
    _tfsFile->set_batch_count(TestGFactory::_batchCount);
  }
  if (TestGFactory::_segSize != 0)
  {
    _tfsFile->set_segment_size(TestGFactory::_segSize);
  }
  g_lock.unlock();
  return 0;
}

void TestTfsCase::tearDown()
{
  delete this;
}

int TestTfsCase::run()
{
  return 0;
}

int TestTfsCase::tfsAssert(const char *msg,bool expect,int result)
{
  bool exam;
  if(result < 0){
    exam = false;
  }else if(result == 0){
    exam = true;
  }
  if(expect != exam){
    TBSYS_LOG(ERROR,"%s : FAILED,ret: %d ",msg,result);
    return -1;
  }
  TBSYS_LOG(ERROR,"%s : SUCCESS",msg);
  return 0;
}

int TestTfsCase::tfsAssertFail(const char *msg, int result)
{
  if(result == 0){
    TBSYS_LOG(ERROR, "%s: FAILED", msg);
  }else{
    TBSYS_LOG(ERROR, "%s: SUCCESS", msg);
  }
  return 0;
}


