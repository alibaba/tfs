#include"tfs_client_init.h"

TEST_F(TfsInit,01_nulink)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,02_nulink_less_size)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=10*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
   EXPECT_EQ(100*(1<<10),con);
	EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,03_nulink_more_size)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=110*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
   EXPECT_EQ(100*(1<<10),con);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,04_nulink_wrong_size)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=-1;
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
   EXPECT_EQ(100*(1<<10),con);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,05_nulink_with_suffix)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,".jpg",NULL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,06_nulink_dif_suffix_1)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,".jpg",NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,07_nulink_dif_suffix_2)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag,".txt");
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,".jpg",NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,08_nulink_null_tfs_name)
{
    
	 int64_t con=0;
	 
	 int64_t ret;
	 
	 ret=tfsclient->unlink(con,NULL,NULL,NULL);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,09_nulink_empty_tfs_name)
{
     char tfs_name[19]="";
	 int64_t con=0;
	 
	 int64_t ret;
	 
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,10_nulink_wrong_tfs_name)
{
     char tfs_name[19]="thjkolijnhbgyuhj";
	 int64_t con=0;
	 
	 int64_t ret;
	 
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,11_nulink_two_times)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,12_nulink_link)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL,UNDELETE);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,13_link)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL,UNDELETE);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,14_conceal_reveal)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL,CONCEAL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL,REVEAL);
	 EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,15_reveal)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 char*local_file;
	 int32_t flag;
	 int64_t con=100*(1<<10);
	 
	 
	 int64_t ret;
	 
	 
	 local_file=a100K;
     flag=T_DEFAULT;	 
	 ret=tfsclient->save_file(tfs_name, tfs_name_len,local_file,flag);
	 EXPECT_EQ(100*(1<<10),ret);
	 cout<<"file name is "<<tfs_name<<" and the length is "<<tfs_name_len<<endl;
	 ret=tfsclient->unlink(con,tfs_name,NULL,NULL,REVEAL);
	 EXPECT_NE(TFS_SUCCESS,ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}


















