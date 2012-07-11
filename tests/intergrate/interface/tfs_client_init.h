#ifndef TFS_CLIENT_INTT_H_
#define TFS_CLIENT_INTT_H_

#include<tfs_client_api.h>
#include<string>
#include "common/define.h"
#include "common/func.h"

#include<gtest/gtest.h>
#include<limits.h>

#include<fstream.h>



#define a1Gjpg    "/home/admin/tfstest_api/resource/1G.jpg"
#define a5Mjpg    "/home/admin/tfstest_api/resource/5M.jpg"
#define a3Mjpg    "/home/admin/tfstest_api/resource/3M.jpg"
#define a2Mjpg    "/home/admin/tfstest_api/resource/2M.jpg"
#define a1Mjpg    "/home/admin/tfstest_api/resource/1M.jpg"
#define a100Kjpg  "/home/admin/tfstest_api/resource/100K.jpg"
#define a10Kjpg   "/home/admin/tfstest_api/resource/10K.jpg"
#define a100K      "/home/admin/tfstest_api/resource/100K"
#define a1M        "/home/admin/tfstest_api/resource/1M"
#define a5M        "/home/admin/tfstest_api/resource/5M"




using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class TfsInit: public testing::Test
{
  protected:
     int ret;
     TfsClient* tfsclient;
     int32_t       cache_time  ; 
     int32_t       cache_items ;
     

     virtual void SetUp()
     {
         cache_time=1800;
         
         cache_items=500000;
         
         const   char* ns_addr = "10.232.4.3:3100"; 
         
         tfsclient=TfsClient::Instance();
         
         ret = tfsclient->initialize(ns_addr,cache_time,cache_items);
         
         if(ret!=TFS_SUCCESS)
         {
           cout<<"tfsclient initialize fail!"<<endl; 
           return;
         }
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
     virtual void data_in(const char *file_dir,char buffer[],long offset,long length)
     {
        FILE *fp=fopen(file_dir,"r+t");
        
        long num=0;
        if(fp==NULL)
        {   
            cout<<"open file error!"<<endl;
            return;
        } 
          
        num=fread(buffer,sizeof(char),length,fp);        
        if(num<=0)
        {
            cout<<"read file error!"<<endl;
        }
        cout<<"the all length is "<<num<<" !!!!!!"<<endl;

        fclose(fp);
     }
	  
     virtual uint32_t getcrc(const char*file_dir,int32_t len)
	   {
	      char*buffer = new char[len];
        uint32_t crcnum=0;
        //Func f = new Func();
		    data_in(file_dir,buffer,0,len);
		    uint32_t Ret = Func::crc(crcnum,buffer,len);
        delete []buffer;
        //delete f;
        return Ret;
     }
	 
	   virtual void chfile(const char*buf,const char*filename)
	  {
	    ofstream out(filename);
	   	out<<buf;
	  	out.close();
	  }
};


#endif


































