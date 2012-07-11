#include"tfs_client_init.h"


TEST_F(TfsInit,01_close_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;

	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,02_close_T_READ)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_READ);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

/*TEST_F(TfsInit,03_close_T_WRITE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_WRITE);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}
*/
TEST_F(TfsInit,04_close_T_CREATE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_CREATE);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,05_close_T_NEWBLK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_NEWBLK);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,06_close_T_NOLEASE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;

	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_NOLEASE);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,07_close_T_STAT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;

	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_STAT);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,08_close_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;

	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a5M,T_LARGE);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_LARGE);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}



TEST_F(TfsInit,09_close_T_UNLINK)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;

	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_UNLINK);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,10_close_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
   
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,tfs_name,tfs_name_len);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,11_close_wrong_len)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
   int32_t con=-1;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,tfs_name,con);
	 EXPECT_NE(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,12_close_more_len)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 int32_t con=100;

	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,tfs_name,con);
	 cout<<"tfs_name is "<<tfs_name<<" !"<<endl;
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,13_close_less_len)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 int32_t con=3;

	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,tfs_name,con);
	 EXPECT_NE(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,14_close_null_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,NULL);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,15_close_empty_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd,"");
	 EXPECT_NE(TFS_SUCCESS,Ret);
	 	 
}

TEST_F(TfsInit,16_close_two_times)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 Ret=tfsclient->close(fd);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,Ret);
	 	 
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}


