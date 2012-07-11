#include"tfs_client_init.h"


TEST_F(TfsInit,01_read_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_read_more_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=110*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_read_less_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=10*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(10*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,04_read_wrong_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=-1;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}


TEST_F(TfsInit,05_read_null_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=0;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, NULL,count);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_read_wrong_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=-1;
	 int64_t ret;
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,07_read_not_open_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=45;
	 int64_t ret;
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,08_read_large)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 static  char buf[1<<30];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a1Gjpg,T_LARGE,NULL,NULL);
	 EXPECT_EQ(1<<30,ret);
	 count=1<<30;
	 
	 suffix=NULL;
	 flags=T_READ|T_LARGE;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(1<<30,ret);
	 
	 tfsclient->close(fd);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}






























