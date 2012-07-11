#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_updata_file_right_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 int Ret ; 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,02_updata_file_right_T_READ)
{
         char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
         char*local_file=a100K;
	 suffix=NULL;
	 flag=T_READ;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,03_updata_file_right_T_WRITE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_WRITE;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 int Ret ; 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,04_updata_file_right_T_CREATE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_CREATE;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
	 
}

TEST_F(TfsInit,05_updata_file_right_T_NEWBLK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_NEWBLK;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);

}

TEST_F(TfsInit,06_updata_file_right_T_NOLEASE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_NOLEASE;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,07_updata_file_right_T_STAT)
{
         char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_STAT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
	
}

TEST_F(TfsInit,08_updata_file_right_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 int64_t con=5*(1<<20);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a5M;
	 suffix=NULL;
	 flag=T_LARGE;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,09_updata_file_right_T_UNLINK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_UNLINK;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);

}



TEST_F(TfsInit,10_updata_file_T_LARGE_with_small)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_LARGE;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,11_updata_file_T_DEFAULT_with_large)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 char*local_file=a5M;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(5*(1<<20),ret);
}

TEST_F(TfsInit,12_updata_file_large)
{
         char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a5M,T_LARGE);
	 EXPECT_EQ(5*(1<<20),ret);
	 
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}


TEST_F(TfsInit,13_updata_file_T_DEFAULT_empty_tfsname)
{
     char*tfs_name="";
	
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1016,ret);
}

TEST_F(TfsInit,14_updata_file_T_DEFAULT_null_tfsname)
{
     char*tfs_name=NULL;
	 
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1016,ret);
}

TEST_F(TfsInit,15_updata_file_T_DEFAULT_wrong_tfsname)
{
     char*tfs_name="oiujntyjuhjjhgohfybuab";
	 
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(-1,ret);
}



TEST_F(TfsInit,16_updata_file_T_DEFAULT_dif_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 char*local_file=a100K;
	 suffix=".jpg";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}

TEST_F(TfsInit,17_updata_file_T_DEFAULT_same_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 int64_t con=200*(1<<10);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a100K;
	 suffix=".jpg";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 int Ret ; 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}

TEST_F(TfsInit,18_updata_file_T_DEFAULT_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 char*local_file=a100K;
	 suffix=".txt";
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(100*(1<<10),ret);
}

TEST_F(TfsInit,19_updata_file_T_DEFAULT_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.232.36.206:9291";
	 int64_t con=200*(1<<10);
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 TfsFileStat file_stat;
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(100*(1<<10),ret);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
	 
	 int Ret ; 
	 Ret=tfsclient->unlink(con, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
}



TEST_F(TfsInit,20_updata_file_T_DEFAULT_not_exist_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.232.523.3:3100";//不存在的服务器

	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(-1,ret);
}

TEST_F(TfsInit,21_updata_file_T_DEFAULT_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr="10.sds.s3.w:sdfs";

	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	
	 char*local_file=a100K;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(-1,ret);
}



TEST_F(TfsInit,22_updata_file_many_times)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 int32_t flag;
	 char*suffix;
	 
	 int64_t ret;
		 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a1M,T_DEFAULT);
	 EXPECT_EQ(1<<20,ret);
	 
	 char*local_file=a1M;
	 suffix=NULL;
	 flag=T_DEFAULT;
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(1<<20,ret);
	 ret=tfsclient->save_file_update(local_file,flag,tfs_name,suffix);
	 EXPECT_EQ(1<<20,ret);

}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}









































