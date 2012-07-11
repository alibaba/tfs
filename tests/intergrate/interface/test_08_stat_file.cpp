#include"tfs_client_init.h"

TEST_F(TfsInit,01_stat_file_with_null_stat)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(NULL,tfs_name,NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
	 
}

//TEST_F(TfsInit,02_stat_file_with_empty_stat)
//{
//     char tfs_name[19];
//	 int32_t tfs_name_len=19;
//	 char*local_file;
//	 int32_t flag;
//	
//	 
//	 int64_t ret;
//	 
//	 local_file=a100K;
//     flag=T_DEFAULT;	 
//	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
//	 EXPECT_EQ(100*(1<<10),ret);
//	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
//	 ret=tfsclient->stat_file("",tfs_name,NULL);
//	 EXPECT_EQ(TFS_ERROR,ret);
//}

TEST_F(TfsInit,03_stat_file_nulink)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 tfsclient->unlink(con,tfs_name,NULL,NULL);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,04_stat_file_nulink_force)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 TfsFileStat file_stat;	
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 tfsclient->unlink(con,tfs_name,NULL,NULL);
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL,FORCE_STAT);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
}

TEST_F(TfsInit,05_stat_file_null_tfs_name)
{
	 
	 int64_t ret;
	  
	 TfsFileStat file_stat;	
	 ret=tfsclient->stat_file(&file_stat,NULL,NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,06_stat_file_empty_tfs_name)
{
	 
	 int64_t ret;
	 
	
	 TfsFileStat file_stat;	
	 ret=tfsclient->stat_file(&file_stat,"",NULL);
	EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,07_stat_file_with_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,".jpg");
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
}

TEST_F(TfsInit,08_stat_file_dif_suffix_1)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,".jpg");
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,09_stat_file_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,".txt");
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,".jpg");
	EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,10_stat_file_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*ns_addr="10.232.4.3:3100";
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL,NORMAL_STAT,ns_addr);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 EXPECT_EQ(100*(1<<10),file_stat.size_);
}


TEST_F(TfsInit,11_stat_file_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*ns_addr="10.232.98.3:3200";//另一个服务器
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL,NORMAL_STAT,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,12_stat_file_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*ns_addr="1hj.u32.4.3:3200";
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL,NORMAL_STAT,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,13_stat_file_not_exist_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 char*ns_addr="1hj.u32.4.3:3400";//没开启的服务器
	 
	 int64_t ret;
	 
	 local_file=a100K;
     flag=T_DEFAULT;
     TfsFileStat file_stat;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->stat_file(&file_stat,tfs_name,NULL,NORMAL_STAT,ns_addr);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}








