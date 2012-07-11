#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_updata_char_right_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
         count=100*(1<<10);
         cout<<"the name is "<<tfs_name<<endl;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,02_updata_char_right_T_READ)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
         count=100*(1<<10);
	 suffix=NULL;
	 flag=T_READ;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,03_updata_char_right_T_WRITE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_WRITE;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,04_updata_char_right_T_CREATE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_CREATE;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);

}

TEST_F(TfsInit,05_updata_char_right_T_NEWBLK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_NEWBLK;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}

TEST_F(TfsInit,06_updata_char_right_T_NOLEASE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_NOLEASE;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,07_updata_char_right_T_STAT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
         count=100*(1<<10);
	 suffix=NULL;
	 flag=T_STAT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);

}

TEST_F(TfsInit,08_updata_char_right_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=100*(1<<10)+5*(1<<20);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a5M,buf,0,5*(1<<20));
     count=5*(1<<20);
	 suffix=NULL;
	 flag=T_LARGE;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix,NULL,a5M);
	 EXPECT_EQ(-1,ret);

}

TEST_F(TfsInit,09_updata_char_right_T_UNLINK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_UNLINK;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}


TEST_F(TfsInit,10_updata_char_T_LARGE_with_small)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 data_in(a100K,buf,0,100*(1<<10));
         count=100*(1<<10);
	 suffix=NULL;
	 flag=T_LARGE;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,11_updata_char_T_DEFAULT_with_large)
{
         char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a5M,buf,0,5*(1<<20));
          count=5*(1<<20);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,12_updata_char_large)
{
         char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a5M,T_LARGE);
	 EXPECT_EQ(5*(1<<20),ret);

	 data_in(a100K,buf,0,100*(1<<10));
         count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,13_updata_char_T_DEFAULT_with_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 int64_t con=110*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
     count=10*(1<<10);
	 char*suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(10*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(10*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}



TEST_F(TfsInit,14_updata_char_T_DEFAULT_wrong_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 data_in(a100K,buf,0,100*(1<<10));
         count=-1;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,15_updata_char_T_DEFAULT_empty_tfsname)
{
     char*tfs_name="";
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1016,ret);
}

TEST_F(TfsInit,16_updata_char_T_DEFAULT_null_tfsname)
{
     char*tfs_name=NULL;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1016,ret);
}

TEST_F(TfsInit,17_updata_char_T_DEFAULT_wrong_tfsname)
{
     char*tfs_name="oiujntyjuhjjhgohfybuab";
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,18_updata_char_T_DEFAULT_empty_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char* buf="";
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);

         count=0;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
	 
}

TEST_F(TfsInit,19_updata_char_T_DEFAULT_null_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char* buf=NULL;
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     count=0;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,20_updata_char_T_DEFAULT_dif_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=".jpg";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}

TEST_F(TfsInit,21_updata_char_T_DEFAULT_same_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=".jpg";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,22_updata_char_T_DEFAULT_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=".txt";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}

TEST_F(TfsInit,23_updata_char_T_DEFAULT_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.232.36.206:9291";
	 int64_t con=200*(1<<10);
	 
	 int64_t ret; 
	 int Ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}


TEST_F(TfsInit,24_updata_char_T_DEFAULT_not_exist_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.232.523.268:3100";//不存在的服务器
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,25_updata_char_T_DEFAULT_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[100*(1<<10)];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.sds.s3.w:sdfs";
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 data_in(a100K,buf,0,100*(1<<10));
     count=100*(1<<10);
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,26_updata_many_times)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char  buf[1<<20];
	 int64_t count;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret; 
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a1M,T_DEFAULT);
	 EXPECT_EQ(1<<20,ret);
	 data_in(a1M,buf,0,1<<20);
         count=1<<20;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(1<<20,ret);
	 ret=tfsclient->save_file_update(buf,count,flag,tfs_name,suffix);
	 EXPECT_EQ(1<<20,ret);

}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}









































