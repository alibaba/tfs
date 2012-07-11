/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_update.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_tfs_update.h"
#include "func.h"
#include "test_gfactory.h"

//XXX:update function not complete
////////////////////////////TestUpdate///////////////////////////////
tbsys::CThreadMutex TestTfsUpdate::_lock;
int TestTfsUpdate::_have_display_statis = 0;
int TestTfsUpdate::_count = 0;
bool TestTfsUpdate::_hasReadFilelist = false;
int TestTfsUpdate::_partSize = 0;
VUINT32 TestTfsUpdate::_crcSet;
VSTRING TestTfsUpdate::_filenameSet;

int TestTfsUpdate::setUp()
{
	TestTfsCase::setUp();
  _currentFile = 0;
  
  TestTfsUpdate::_lock.lock();  
  TestGFactory::_index ++;
  /* Set localkey */
  _keyIndex = TestGFactory::_index;  
  char *filelist = (char *)CConfig::getCConfig().getString("tfswrite","seedlist_name");
  
  TBSYS_LOG(DEBUG,"filelist = %s", filelist);
  if(filelist == NULL)
  {
    filelist = "./tfsseed_file_list.txt";
  }

  _loopFlag = CConfig::getCConfig().getInt("tfsupdate","loop_flag"); 
  _writeCount = CConfig::getCConfig().getInt("tfsupdate","writeCount"); 
  _unitMin = CConfig::getCConfig().getInt("tfsupdate","unit_min"); 
  _unitMax = CConfig::getCConfig().getInt("tfsupdate","unit_max"); 
  //_largeFlag = CConfig::getCConfig().getInt("tfsupdate","largeFlag"); 
  //_writeVerifyFlag = CConfig::getCConfig().getInt("tfsupdate","writeVerifyFlag");
  _unlinkRatio = CConfig::getCConfig().getInt("tfsupdate","unlinkRatio");

  /* Init */ 
  memset( (char *)_localKey, 0x00, LOCALKEYLEN );
  sprintf( _localKey, "%s%d", "./localkey/",TestGFactory::_index );

  /* get self's threadNo from global factory */
  _threadNo = TestGFactory::_threadNo++;
  if (TestTfsUpdate::_hasReadFilelist == false)
  {
    TestCommonUtils::readFilelist(filelist, _crcSet, _filenameSet);
    TestTfsUpdate::_hasReadFilelist = true;
  }
  TestTfsUpdate::_partSize = _crcSet.size() / TestGFactory::_threadCount;
  _lockFlag = 0;
  TestTfsUpdate::_lock.unlock();
  TestCommonUtils::getFilelist(_threadNo, _partSize, _crcSet, _crcSetPerThread,
    _filenameSet, _filenameSetPerThread);

  /* choose a random number between _unitMin and _unitMax as write unit */
  srand(time(NULL)+rand()+pthread_self());
  _unit = _unitMin + (int) ((_unitMax - _unitMin + 1.0) * (rand() / (RAND_MAX + 1.0)));
  TBSYS_LOG(ERROR, "_unit : %d", _unit);
  _data = new char [_unit + 1];

  TestCommonUtils::generateData(_data, _unit);

	return 0;
}

int TestTfsUpdate::writeData(char *data,int size)
{

	int left = size;
	int nwrite = 0; 
	int ret = -1;

	while(left > 0)
  {
		ret = _tfsFile->write( _fd, data + nwrite, left);
		if(ret < 0)
    {
			TBSYS_LOG(ERROR,"write failed:%d",ret);
			break;
		} else if(ret > left){
			ret = left;
			break;
		} else {
			left -= ret;
			nwrite += ret;
		}
	}

	return ret;
}

int TestTfsUpdate::testWrite()
{
	int ret  = -1;
  uint32_t offset = 0;
  TfsFileStat info;

  if(_filenameSetPerThread.size() == 0){
    TBSYS_LOG(INFO,"File name list is empty");
    return 0;
  }
  
  _tfsFilename = _filenameSetPerThread.at(_currentFile).c_str();
  uint32_t preCrc = _crcSetPerThread[ _currentFile ];
  const char *postfix = strlen(_tfsFilename) > 18 ? (char *)(_tfsFilename + 18) : NULL;

  if (*_tfsFilename == 'T')
  {
    _largeFlag = 0;
    ret = _tfsFile->open((char *)_tfsFilename,(char *)postfix, T_WRITE);
  }
  else if (*_tfsFilename == 'L') 
  {
    _largeFlag = 1;
    ret = _tfsFile->open((char *)_tfsFilename,(char *)postfix, T_WRITE | T_LARGE, _localKey);
  }
  else
  {
    TBSYS_LOG(ERROR, "Invalid File Name!");
    return ret;
  }

  if (ret <= 0)
  {
    TBSYS_LOG(ERROR,"Open failed:%d",ret);
    return ret;
  } else {
    _fd = ret;
  }
  
  ret = _tfsFile->fstat( _fd, &info, FORCE_STAT );
  if(ret != 0)
  {
    TBSYS_LOG(ERROR,"Fstat failed:%d",ret);
    _tfsFile->close( _fd );
    return ret;
  }

  TBSYS_LOG( INFO,"info.size_ = %ld", info.size_ );
  TBSYS_LOG( INFO,"preCrc = %u", preCrc );
  TBSYS_LOG( INFO,"filename = %s", _tfsFilename );
 
  // seek to random offset to write
  for(int iLoop = 0; iLoop < _writeCount; iLoop ++)
  {
    srand(time(NULL) + rand() + pthread_self());
    offset = (uint32_t) ((info.size_ + 1.0) * (rand() / (RAND_MAX + 1.0)));
    
    ret = _tfsFile->lseek(_fd, offset, SEEK_SET);
    if(ret != 0)
    {
      TBSYS_LOG(ERROR,"Lseek failed:%d", ret);
      _tfsFile->close( _fd );
      return ret;
    } 
    ret = writeData( _data, _unit );
    if(ret < 0 )
    {
      TBSYS_LOG(ERROR,"tfs_write failed:%d",ret);
      return ret;
    }
  }

	ret = _tfsFile->close(_fd);
  
	return ret;
}

int TestTfsUpdate::testRead()
{
	char data[MAX_READ_SIZE];
	int nread = 0;
  _crc = 0;  // regenerate crc
	int64_t _totalSize = 0;
	TfsFileStat info;

  memset( (char *)&info, 0x00, sizeof(TfsFileStat) );

  int ret;
  if (_largeFlag == 1)
    {
	  	ret = _tfsFile->open((char *)_tfsFilename, (char *)".jpg", T_READ|T_LARGE);
    } else {
	  	ret = _tfsFile->open((char *)_tfsFilename, (char *)".jpg", T_READ);
    }

	if(ret <= 0)
  {
		TBSYS_LOG(ERROR,"tfs_open failed:%d",ret);
		return ret;
	} else {
    _fd = ret;
  }

	ret = _tfsFile->fstat( _fd, &info, FORCE_STAT );
	if(ret != 0)
  {
		TBSYS_LOG(ERROR,"tfs_stat failed:%d",ret);
		return ret;
	}

	while(nread < info.size_) 
  {
		int rlen = _tfsFile->read( _fd, data, MAX_READ_SIZE );
		if (rlen < 0) 
    {
			TBSYS_LOG(DEBUG, "read tfsfile fail: %d\n", rlen);
			_tfsFile->close( _fd );
			return rlen;
		}

		if (rlen == 0) break;

		_crc = Func::crc(_crc, data, rlen);

		_totalSize += rlen;

		nread += rlen;

	}

	_tfsFile->close( _fd );
  
  /*if (_crc != _preCrc)
  {
    TBSYS_LOG( ERROR, "CRC is different: %u <> %u", _crc, _preCrc );
    ret = -1;
  }*/

	if(_totalSize != info.size_){
    TBSYS_LOG( ERROR, "File size is different: %d <> %d", _totalSize, info.size_ );
		ret = -1;
	}

	return ret;
}

int TestTfsUpdate::testUnlink()
{
  int ret = 0;

  TfsFileStat info;
  memset( (char *)&info, 0x00, sizeof(TfsFileStat) );

  const char *postfix = strlen(_tfsFilename) > 18 ? (char *)(_tfsFilename + 18) : NULL;
  int64_t fileSize = 0;
  ret = _tfsFile->unlink(_tfsFilename, postfix, fileSize);

  if (ret != 0)
  {
    TBSYS_LOG(ERROR, "Unlink failed:%d, fileSize:%d", ret, fileSize);
    return ret;
  }
  if (_largeFlag)
  {
    ret = _tfsFile->open((char *)_tfsFilename,(char *)postfix, T_READ | T_LARGE);                                                                     
  } else {
    ret = _tfsFile->open((char *)_tfsFilename,(char *)postfix, T_READ);
  }

  if (ret <= 0)
  {
    TBSYS_LOG(ERROR,"Open failed:%d",ret);
    return -1;
  } else {
    _fd = ret;
  }

  ret = _tfsFile->fstat( _fd, &info, FORCE_STAT );
  if(ret != 0)
  {
    TBSYS_LOG(ERROR,"Fstat failed:%d",ret);
    _tfsFile->close( _fd );
    return ret;
  }
  _tfsFile->close( _fd );

  if (info.flag_ == 0)
  {
    TBSYS_LOG(ERROR,"Unlink faild: delete falg = %d", info.flag_);
    return -1;
  }
  _alreadyUnlinked = true;
  return ret;
}

int TestTfsUpdate::run()
{
	int ret = -1;
	int64_t start_time = 0;
	int64_t end_time = 0;
  char strCrc[ 12 ];
  _alreadyUnlinked = false;

  memset( (char *)strCrc, 0x00, 12 );

  int fileNo = (int)_filenameSetPerThread.size();

  if ((int)_currentFile < fileNo)
  {
    if( (ret = testWrite()) != 0 )
    {
      TBSYS_LOG(ERROR,"TestUpdate::testWrite: FAILED: %d", ret);
    } else {
      TBSYS_LOG(INFO,"TestUpdate::testWrite: SUCCESSFUL");
    }

    end_time = CTimeUtil::getTime();
    TestGFactory::_statisForWrite.addStatis(start_time, end_time, ret, "writeFile");
    

    if ( (ret = testRead()) != 0)
    {
      TBSYS_LOG(ERROR,"TestUpdate::testRead: FAILED: %d", ret);
    }  else {
      ret = 0;
      TBSYS_LOG(INFO,"TestUpdate::testRead: SUCCESSFUL");
    }

    if (_unlinkRatio)
    {
      start_time = 0;
      srand(time(NULL)+rand()+pthread_self());
      int randNo = rand() % 100;
      if (randNo < _unlinkRatio)
      {
        if ( (ret = testUnlink()) != 0)
        {
          TBSYS_LOG(ERROR,"TestUpdate::testUnlink: FAILED: %d", ret);
        }  else {
          ret = 0;
          TBSYS_LOG(INFO,"TestUpdate::testUnlink: SUCCESSFUL");
        }

        end_time = CTimeUtil::getTime();
        TestGFactory::_statisForUnlink.addStatis(start_time, end_time, ret, "unlinkFile");
      }
    }
  
    if (_alreadyUnlinked == false)
    { 
      // Edit the filename and new crc 
      snprintf( strCrc, CRCSIZE, "%u", _crc );
      std::string record = _tfsFilename;
      record = record + " " + strCrc;
      /* Save it */
      _recordSet.insert( record );
    }

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

  return ret;
}

int TestTfsUpdate::saveFilename()
{
  FILE *fp = NULL;
  char *filelist = (char *)CConfig::getCConfig().getString("tfswrite",
         "writelist_name");

  if(filelist == NULL){
    filelist = "./write_file_list.txt";                                                                                                                
  }

  if((fp = fopen(filelist,"a")) == NULL)
  {
      TBSYS_LOG(ERROR,"open write_file_list failed.");
      return -1;
  }
  std::set<std::string>::iterator it=_recordSet.begin();
  for(; it != _recordSet.end(); it++)
  {
    fprintf(fp,"%s\n",it->c_str());
  }

  fflush(fp);
  fclose(fp);

  return 0;
}

void TestTfsUpdate::tearDown()
{
	delete []  _data;
	_data = NULL;

	bool need_display = false;

	TestTfsUpdate::_lock.lock();
	if(_have_display_statis == 0)
	{
		TBSYS_LOG(INFO,"Statis will be displayed.");
		_have_display_statis += 1;
		need_display = true;
	}

  /* Save the record to file */
  int iRet = saveFilename();  
  if (iRet != 0)
  {
        TBSYS_LOG(ERROR,"It's failed to save file name list !!!");
  }
	TestTfsUpdate::_lock.unlock();

	if(need_display){
		TBSYS_LOG(INFO,"-------------------------------write statis-------------------------------------");
		TestGFactory::_statisForWrite.displayStatis("write statis");
    /*if (_writeVerifyFlag)
    {
	    TBSYS_LOG(INFO,"-------------------------------read statis-------------------------------------");
		  TestGFactory::_statisForRead.displayStatis("read statis");
    }*/
    if (_unlinkRatio)
    {
      TBSYS_LOG(INFO,"-------------------------------unlink statis-------------------------------------");
		  TestGFactory::_statisForUnlink.displayStatis("unlink statis");
    } 
  	TBSYS_LOG(INFO,"------------------------------------END-----------------------------------------");
	}

	TestTfsCase::tearDown();
}

