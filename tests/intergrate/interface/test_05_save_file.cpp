#include"tfs_client_init.h"

TEST_F(TfsInit,01_save_file_with_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_DEFAULT;
     cout<<"!!!!!@@@@@@@@@@@@@"<<local_file <<"@@@@@@@@@@@@@@@!!!!!!!"<<endl;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,02_save_file_with_T_READ)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_READ;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,03_save_file_with_T_WRITE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_WRITE;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,04_save_file_with_T_CREATE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_CREATE;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,05_save_file_with_T_NEWBLK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_NEWBLK;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,06_save_file_with_T_NOLEASE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_NOLEASE;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,07_save_file_with_T_STAT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_STAT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,08_save_file_with_T_UNLINK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_UNLINK;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,09_save_file_with_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 int64_t count=1<<30;
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a1Gjpg;
     flag=T_LARGE;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,NULL,NULL);
	 EXPECT_EQ(1<<30,ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(1<<30,file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,10_save_file_with_T_LARGE_wrong)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	
	 local_file=a100K;
     flag=T_LARGE;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_NE(TFS_SUCCESS,ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}



TEST_F(TfsInit,11_save_file_with_T_DEFAULT_with_the_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.3:3100";
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,suffix,ns_addr);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,12_save_file_with_T_DEFAULT_with_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.85:3100";
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,suffix,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
	 
}

TEST_F(TfsInit,13_save_file_with_T_DEFAULT_with_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=NULL;
	 char*ns_addr="1sds.2dsd.s.dsds:sds00";
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,suffix,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}


TEST_F(TfsInit,14_save_file_large_with_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	 local_file=a5M;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_NE(TFS_SUCCESS,ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
}

TEST_F(TfsInit,16_save_file_with_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix=".jpg";
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100Kjpg;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,17_save_file_dif_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*suffix="";
	 int64_t count=100*(1<<10);
	 
	 int64_t ret;
	 int Ret;
	 
	 TfsFileStat file_stat;
	 local_file=a100Kjpg;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}





int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}























