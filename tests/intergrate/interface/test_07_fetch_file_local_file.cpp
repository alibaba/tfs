#include"tfs_client_init.h"



TEST_F(TfsInit,01_fetch_local_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
   uint32_t crc;
   uint32_t tempcrc;
	 char*local_file;
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
   
}



TEST_F(TfsInit,02_fetch_local_wrong_tfsname)
{
     char tfs_name[19]="twyujikolpoiujikmn";
	 
	 char*suffix=NULL;

	 char*local_file;
	 
	 int64_t ret;
	 
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}



TEST_F(TfsInit,03_fetch_local_with_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=".jpg";
	
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,04_fetch_local_dif_suffix_1)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=".jpg";
	 
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,05_fetch_local_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=".jpg";
	 
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,".txt");
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,06_fetch_local_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.3:3100";
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix,ns_addr);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,07_fetch_local_with_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	 char*ns_addr1="10.232.4.3:3100";
	 char*ns_addr2="10.232.4.89:3100";
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT,NULL,ns_addr1);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix,ns_addr2);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,08_fetch_local_not_exit_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	 char*ns_addr="10.232.4.3:3300";//不存在的服务器
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,09_fetch_local_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	 char*ns_addr="10.sds.s.3:3300";
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,10_fetch_null_local)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file=NULL;
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,11_fetch_empty_local)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,12_fetch_wrong_local)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 local_file="`/`!@/";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,13_fetch_large)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*suffix=NULL;
	 
	 char*local_file;
	 
	 int64_t ret;
	 ret=tfsclient->save_file(tfs_name,tfs_name_len, a1Gjpg,T_LARGE,NULL,NULL);
	 EXPECT_EQ(1<<30,ret);
	 local_file="TEMP";
	 ret=tfsclient->fetch_file(local_file,tfs_name,suffix);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}










