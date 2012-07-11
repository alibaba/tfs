/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_common_utils.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_common_utils.h"
#include "test_gfactory.h"

int TestCommonUtils::generateData(char *data, int size)
{
	srand(time(NULL)+rand()+pthread_self());

	for(int i = 0; i < size; i ++) 
  {
		data[ i ] = rand() % 90 + 32;
	}
  data[ size ] = 0;

	return size;
}

int TestCommonUtils::readFilelist(char *filelist, VUINT32& crcSet, VSTRING& filenameSet)
{

  FILE *fp = NULL;
  if((fp = fopen(filelist,"r")) == NULL)
  {
    TBSYS_LOG(DEBUG,"open file_list failed.");
    return -1;
  }
  uint32_t crc = 0;
  char filename[64];
  while(fgets(filename,sizeof(filename),fp))
  {
    if(filename[strlen(filename)-1] == '\n')
    {
      filename[strlen(filename)-1] = '\0';
    } else {
      filename[strlen(filename)] = '\0';
    }
#if 0
    TBSYS_LOG(ERROR,"line = %d, filename = %s, filelist = %s!!!",__LINE__,filename, filelist);
#endif
    char *p = strchr(filename,' ');
    *p++ = '\0';
    sscanf(p,"%u",&crc);

    filenameSet.push_back(filename);
    crcSet.push_back(crc);

  }

  return 0;
}


int TestCommonUtils::getFilelist(int partNo, int partSize, VUINT32& crcSet, 
      VUINT32& crcSetPerThread, VSTRING& filenameSet, VSTRING& filenameSetPerThread)
{
  // total size less than thread count
  if (partSize == 0)
  {
    partSize = 1; 
  }
  
  int offset = partNo * partSize;
  int end = (offset + partSize) > (int)crcSet.size()? crcSet.size():(offset + partSize); 
  for (; offset < end; offset++)
  {
    crcSetPerThread.push_back(crcSet[offset]);
    filenameSetPerThread.push_back( filenameSet.at(offset).c_str() );
  }

  // the last thread eat the left
  if (partNo == (TestGFactory::_threadCount - 1) )
  {
    for (; offset < (int)crcSet.size(); offset++)
    {
      crcSetPerThread.push_back(crcSet[offset]);
      filenameSetPerThread.push_back( filenameSet.at(offset).c_str() );
    }
  }

  //debug
  /*
  char fileListPerThread[20] = {0};
  sprintf(fileListPerThread, "./read_file_list_%d.txt", partNo);
  FILE *fp = fopen(fileListPerThread, "w");
  VSTRING::iterator it = filenameSetPerThread.begin();
  for (; it != filenameSetPerThread.end(); it++)
  {
     fprintf(fp, "%s\n", it->c_str());
  } 
  fflush(fp);
  fclose(fp);
  */
  return 0;

}
