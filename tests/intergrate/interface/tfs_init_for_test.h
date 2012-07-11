#ifndef TFS_INIT_FOR_TEST_H
#define TFS_INIT_FOR_TEST_H


#include<tfs_client_api.h>
#include<string>
#include<gtest/gtest.h>
#include<limits.h>

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class TfsInitTest: public testing::Test
{
  protected:
    TfsClient* tfsclient;  
    bool ret;
    virtual void SetUp()
  	{
	    tfsclient=TfsClient::Instance();
   	}
  	virtual void TearDown()
  	{
	    ret = tfsclient->destroy();
         
         if(ret!=TFS_SUCCESS)
         {
           cout<<"tfsclient destroy fail!"<<endl;
           return;
        	}
    }
};
#endif
