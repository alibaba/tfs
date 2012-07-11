#include"tfs_client_init.h"

TEST_F(TfsInit,01_save_char_with_T_DEFAULT)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,100*(1<<10));
   ASSERT_STRNE(buf,NULL);
   count=100*(1<<10);
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(100*(1<<10),file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,02_save_char_with_T_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;

   int64_t ret;
                                                               
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_READ;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
  char*suffix=NULL;
    TfsFileStat file_stat;
    int Ret;
    Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
    EXPECT_EQ(TFS_SUCCESS,Ret);
    EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
    ASSERT_NE(TFS_ERROR,Ret);

}

TEST_F(TfsInit,03_save_char_with_T_WRITE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_WRITE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,04_save_char_with_T_CREATE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_CREATE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,05_save_char_with_T_NEWBLK)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_NEWBLK;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,06_save_char_with_T_NOLEASE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_NOLEASE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,07_save_char_with_T_STAT)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_STAT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,08_save_char_with_T_UNLINK)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_UNLINK;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(100*(1<<10),ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,09_save_char_with_T_LARGE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   static  char buf[1<<30];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a1Gjpg,buf,0,1<<30);
   ASSERT_STRNE(buf,NULL);
   count=1<<30;
   flag=T_LARGE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag, NULL, NULL, "/home/admin/.bashrc");
   EXPECT_EQ(1<<30,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(1<<30,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,10_save_char_with_T_LARGE_wrong)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;

   int64_t ret;
                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_LARGE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;

}

TEST_F(TfsInit,11_save_char_with_T_DEFAULT_more_count)
{
    char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;

   int64_t ret;

                                                                   
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400+1024;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(TFS_ERROR,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,12_save_char_with_T_DEFAULT_wrong_count)
{
    char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;

   int64_t ret;

                                                                    
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=-1;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,13_save_char_with_T_DEFAULT_right_count)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;

   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=1024*50;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(1024*50,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(1024*50,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,14_save_char_with_T_DEFAULT_NULL_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t count;
   int32_t flag;

   int64_t ret;
                                                                 
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, NULL, count,flag);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,15_save_char_with_T_DEFAULT_empty_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400]="";
   int64_t count;
   int32_t flag;

   int64_t ret;
                                                                 
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,16_save_char_with_T_DEFAULT_with_the_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[102400];
	 int64_t count;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.3:3100";
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
	 ASSERT_STRNE(buf,NULL);
     count=100*(1<<10);
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,17_save_char_with_T_DEFAULT_with_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[102400];
	 int64_t count;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.89:3200";
	 
	 int64_t ret;
	 
	 data_in(a100K,buf,0,100*(1<<10));
	 ASSERT_STRNE(buf,NULL);
     count=100*(1<<10);
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,18_save_char_with_T_DEFAULT_with_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[102400];
	 int64_t count;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="1sdka.2s2.s.s:3200";
	 
	 int64_t ret;
	 

	 data_in(a100K,buf,0,100*(1<<10));
	 ASSERT_STRNE(buf,NULL);
     count=100*(1<<10);
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr);
cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 EXPECT_NE(TFS_SUCCESS,ret);
}


TEST_F(TfsInit,19_save_char_with_T_DEFAULT_with_suffix)
{
  char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=".jpg";


   int64_t ret;
   int Ret;

   TfsFileStat file_stat;
   
   data_in(a100Kjpg,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix);
   EXPECT_EQ(102400,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, suffix,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}



TEST_F(TfsInit,20_save_char_with_T_DEFAULT_dif_suffix)
{
  char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=".txt";


   int64_t ret;
   int Ret;

   TfsFileStat file_stat;
   
   data_in(a100Kjpg,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix);
   EXPECT_EQ(102400,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, suffix,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,21_save_large_char_with_T_DEFAULT)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[5*(1<<20)];
   int64_t count;
   int32_t flag;
   

   int64_t ret;                                                                 
   data_in(a5M,buf,0,5*(1<<20));
   ASSERT_STRNE(buf,NULL);
   count=5*(1<<20);
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag);
   EXPECT_EQ(TFS_ERROR,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,22_save_char_with_T_DEFAULT_with_key)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;
   char*ns_addr=NULL;
   char*key=a100K;
   
   int64_t ret;
   int Ret;

   TfsFileStat file_stat;                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr,key);
   EXPECT_EQ(102400,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
   Ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
   EXPECT_EQ(TFS_SUCCESS,Ret);
   EXPECT_EQ(102400,file_stat.size_);

   Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
   ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,23_save_char_with_T_DEFAULT_empty_key)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[102400];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;
   char*ns_addr=NULL;
   char*key="";
   
   int64_t ret;
                                                                 
   data_in(a100K,buf,0,102400);
   ASSERT_STRNE(buf,NULL);
   count=102400;
   flag=T_DEFAULT;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr,key);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,24_save_char_with_T_DEFAULT_wrong_key)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[5*(1<<20)];
   int64_t count;
   int32_t flag;
   char*suffix=NULL;
   char*ns_addr=NULL;
   char*key="skdfnhska";
   
   int64_t ret;
                                                                 
   data_in(a5M,buf,0,5*(1<<20));
   ASSERT_STRNE(buf,NULL);
   count=5*(1<<20);
   flag=T_LARGE;
   ret=tfsclient->save_file(tfs_name, tfs_name_len, buf, count,flag,suffix,ns_addr,key);
   EXPECT_NE(TFS_SUCCESS,ret);
   cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}


int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}






















