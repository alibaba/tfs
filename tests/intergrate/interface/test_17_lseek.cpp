#include"tfs_client_init.h"



TEST_F(TfsInit,01_lseek_set)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=T_SEEK_SET;
	 int64_t offset=0;
	 ret=   tfsclient-> lseek(fd,offset,whence);
     ASSERT_EQ(0,ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_lseek_cur)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 

	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=T_SEEK_CUR;
	 int64_t offset=0;
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_EQ(0,ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_lseek_wrong_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=T_SEEK_SET;
	 int64_t offset=-1;
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_NE(-1,ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,04_lseek_more_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=T_SEEK_SET;
	 int64_t offset=110*(1<<10);
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_NE(110*(1<<10),ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,08_lseek_less_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=T_SEEK_SET;
	 int64_t offset=10*(1<<10);
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_EQ(10*(1<<10),ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,05_lseek_wrong_whence)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 int whence=5;
	 int64_t offset=0;
	 ret=tfsclient-> lseek(fd,offset,whence);
   ASSERT_NE(0,ret);

	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_lseek_wrong_fd)
{
     int fd=-1;
	 int whence=T_SEEK_SET;
	 int64_t offset=0;
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_NE(0,ret);
}

TEST_F(TfsInit,07_lseek_not_open_fd)
{
     int fd=59;
	 int whence=T_SEEK_SET;
	 int64_t offset=0;
	 ret=tfsclient-> lseek(fd,offset,whence);
     ASSERT_NE(0,ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}










