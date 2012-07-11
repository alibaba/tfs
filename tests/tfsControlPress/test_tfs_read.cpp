/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_read.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "func.h"
#include "test_gfactory.h"
#include "test_tfs_statis.h"
#include "test_tfs_read.h"

#define zongluo 1
using namespace tbsys;

//////////////////////////TestTfsRead///////////////////////////////////

int TestTfsRead::_have_display_statis = 0;
int TestTfsRead::_count = 0;
bool TestTfsRead::_hasReadFilelist = false;
int TestTfsRead::_partSize = 0;
CThreadMutex TestTfsRead::_lock;
VUINT32 TestTfsRead::_crcSet;
VSTRING TestTfsRead::_filenameSet;

int TestTfsRead::setUp()
{
  srand(time(NULL) + pthread_self());
  TestTfsCase::setUp();
  _currentFile = 0;
  char *filelist = (char *)CConfig::getCConfig().getString("tfsread","filelist_name");
  _largeFlag = CConfig::getCConfig().getInt("tfsread","largeFlag");
  _loopFlag = CConfig::getCConfig().getInt("tfsread","loop_flag");
  _unit = CConfig::getCConfig().getInt("tfsread","unit"); 
  _data = new char [_unit];

  TBSYS_LOG(DEBUG,"filelist = %s", filelist);
  if(filelist == NULL)
  {
    filelist = "./tfsseed_file_list.txt";
  }
  TestTfsRead::_lock.lock();
  /* get self's threadNo from global factory */
  _threadNo = TestGFactory::_threadNo++;
  if (TestTfsRead::_hasReadFilelist == false)
  {
    TestCommonUtils::readFilelist(filelist, _crcSet, _filenameSet);
    TestTfsRead::_hasReadFilelist = true;
  }
  TestTfsRead::_partSize = _crcSet.size() / TestGFactory::_threadCount;
  _lockFlag = 0;
  TestTfsRead::_lock.unlock();
  TestCommonUtils::getFilelist(_threadNo, _partSize, _crcSet, _crcSetPerThread,
    _filenameSet, _filenameSetPerThread);

  return 0;
}

int TestTfsRead::testRead()
{
  int ret = 0;
  int fd = 0;
  int64_t nread = 0;
  uint32_t crc = 0;
  TfsFileStat info;

  memset( (char *)&info, 0x00, sizeof(TfsFileStat) );
  _totalSize = 0;

  if(_filenameSetPerThread.size() == 0){
    TBSYS_LOG(INFO,"File name list is empty");
    return 0;
  }
  
  const char *tfsFilename = _filenameSetPerThread.at(_currentFile).c_str();
  uint32_t preCrc = _crcSetPerThread[ _currentFile ];
  const char *postfix = strlen(tfsFilename) > 18 ? (char *)(tfsFilename + 18) : NULL;

  if (_largeFlag)
  {
    ret = _tfsFile->open((char *)tfsFilename,(char *)postfix, T_READ | T_LARGE);
  } else {
    ret = _tfsFile->open((char *)tfsFilename,(char *)postfix, T_READ);
  }

  if (ret <= 0)
  {
    TBSYS_LOG(ERROR,"Open failed:%d",ret);
    return -1;
  } else {
    fd = ret;
  }

  ret = _tfsFile->fstat( fd, &info, FORCE_STAT );
  if(ret != 0)
  {
    TBSYS_LOG(ERROR,"Fstat failed:%d",ret);
    _tfsFile->close( fd );
    return ret;
  }

  TBSYS_LOG( INFO,"info.size_ = %ld", info.size_ );
  TBSYS_LOG( INFO,"_preCrc = %u", preCrc );
  TBSYS_LOG( INFO,"filename = %s", tfsFilename );

  while(nread < info.size_) 
  {
    int64_t rlen = _tfsFile->read( fd, _data, _unit );
    if (rlen < 0) 
    {
      TBSYS_LOG(DEBUG, "read tfsfile fail: %d\n", rlen);
      _tfsFile->close( fd );
      return rlen;
    }

    if (rlen == 0) break;

    crc = Func::crc(crc, _data, rlen);

    _totalSize += rlen;

    nread += rlen;

  }

  _tfsFile->close( fd );

  if (crc != preCrc)
  {
    TBSYS_LOG( ERROR, "CRC is different: %u <> %u", crc, preCrc );
    ret = -1;
  }

  if(_totalSize != info.size_){
    TBSYS_LOG( ERROR, "File size is different: %ld <> %ld", _totalSize, info.size_ );
    ret = -1;
  }

  return ret;
}

int TestTfsRead::run()
{
  int iRet = 0;
  int64_t end_time = 0;
  int64_t start_time = 0;
  int fileNo = (int)_filenameSetPerThread.size();

  if ((int)_currentFile < fileNo)
  {
    iRet = testRead();
    if (iRet == 0)
    {
      TBSYS_LOG( INFO, "Read file SUCCESS." );
    }
    end_time = CTimeUtil::getTime();

    TestTfsRead::_lock.lock();
    TestGFactory::_statisForRead.addStatis(start_time, end_time, iRet, "readFile");

    TestTfsRead::_lock.unlock();
    _currentFile++;
    if (_loopFlag && _currentFile == fileNo) 
    {
      _currentFile = 0;
    }
  } else {
   if ((_lockFlag == 0) && _lock.trylock())
   {
      _count ++;
      _lockFlag = 1;
      _lock.unlock(); 
   }
  }

  if(_count == TestGFactory::_threadCount)
  {
    TBSYS_LOG(INFO,"stop");
    TestGFactory::_tfsTest.stop();
  }

  return 0;
}

void TestTfsRead::tearDown()
{
  TBSYS_LOG(DEBUG,"tearDown");
  bool need_display = false;

	delete []  _data;
	_data = NULL;

  TestTfsRead::_lock.lock();
  if (TestTfsRead::_have_display_statis == 0) {
    TestTfsRead::_have_display_statis += 1;
    need_display = true;
  }
  TestTfsRead::_lock.unlock();

  if (need_display) {
    TBSYS_LOG(INFO, "-------------------------------read statis-------------------------------------");
    TestGFactory::_statisForRead.displayStatis("read statis");
    TBSYS_LOG(INFO, "-------------------------------END----------------------------------------------");
  }
  TestTfsCase::tearDown();
}

