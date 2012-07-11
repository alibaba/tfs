#include"tfs_client_init.h"



TEST_F(TfsInit,01_fetch_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),ret_count);
}

TEST_F(TfsInit,02_fetch_null_tfsname)
{
    
	 
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 
	 int64_t ret;
	
	 count=0;
	 ret=tfsclient->fetch_file(ret_count,buf,count,NULL,suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,03_fetch_empty_tfsname)
{
    
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 int64_t ret;

	 count=0;
	 ret=tfsclient->fetch_file(ret_count,buf,count,"",suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,04_fetch_wrong_tfsname)
{
     char tfs_name[19]="tyhjuiklomjimkuhy";
	 
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 
	 count=0;
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,05_fetch_more_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=1*(1<<20);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),ret_count);
}

TEST_F(TfsInit,06_fetch_wrong_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=-1;
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,07_fetch_less_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=10*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(10*(1<<10),ret_count);
}





TEST_F(TfsInit,08_fetch_with_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=".jpg";
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),ret_count);
}

TEST_F(TfsInit,09_fetch_dif_suffix_1)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=".jpg";
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,10_fetch_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=".jpg";
	 int64_t ret_count=0;
	
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".txt");
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_GT(0,ret);
}

TEST_F(TfsInit,11_fetch_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.3:3100";
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,12_fetch_with_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 char*ns_addr1="10.232.4.3:3100";
	 char*ns_addr2="10.232.4.98:3100";
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,NULL,ns_addr1);
	 EXPECT_EQ(100*(1<<10),ret);
     printf("dd %s\n", tfs_name);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix,ns_addr2);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,13_fetch_not_exit_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.89:3300";//不存在的服务器
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,14_fetch_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char buf[100*(1<<10)];
	 int64_t count;
	 char*suffix=NULL;
	 char*ns_addr="10.sds.s.3:3300";
	 int64_t ret_count=0;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,15_fetch_large)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
     static char buf[1<<30];
	 int64_t count;
     char*suffix=NULL;
	 int64_t ret_count=0;
	 
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a1Gjpg,T_LARGE,NULL,NULL);
	 EXPECT_EQ(1<<30,ret);
	 count=1<<30;
	 ret=tfsclient->fetch_file(ret_count,buf,count,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(1<<30,ret_count);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}
















