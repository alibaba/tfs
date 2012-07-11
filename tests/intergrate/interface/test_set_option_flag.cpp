#include"tfs_client_init.h"


TEST_F(TfsInit,01_set_flag_T_DEFAULT)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
   int fdd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT), T_DEFAULT);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fdd);
	 
}

TEST_F(TfsInit,02_set_flag_T_READ)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_READ);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_set_flag_T_WRITE)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_WRITE);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,04_set_flag_T_CREATE)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_CREATE);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,05_set_flag_T_NEWBLK)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_NEWBLK);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_set_flag_T_NOLEASE)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_NOLEASE);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,07_set_flag_T_STAT)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_STAT);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,08_set_flag_T_LARGE)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_LARGE);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,09_set_flag_T_UNLINK)
{
     char*tfs_name;
	 int32_t tfs_name_len;
	 int32_t flag;
	 char*suffix;
	 char*ns_addr;
	 
	 int64_t ret;
	 int fd;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 fd=tfsclient->open(tfs_name,NULL, NULL,T_DEFAULT);
	 Ret=tfsclient->set_option_flag(fd, T_UNLINK);
	 EXPECT_EQ(TFS_SUCCESS,Ret);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,10_set_flag_wrong_fd)
{
	 int fd=-1;
	 int Ret;
	 Ret=tfsclient->set_option_flag(fd, T_DEFAULT);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,Ret);
	 
}

TEST_F(TfsInit,11_set_flag_not_exist_fd)
{
	 int fd=100;
	 int Ret;
	 Ret=tfsclient->set_option_flag(fd, T_DEFAULT);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,Ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}





























