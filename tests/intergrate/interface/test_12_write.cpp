#include"tfs_client_init.h"



TEST_F(TfsInit,01_write_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count=100*(1<<10);
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)]; 
	 
	 int fd;
	 int64_t ret;
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,02_write_with_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count=50*(1<<10);
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_EQ(50*(1<<10),ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,03_write_wrong_count)
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
	 
	 count=-1;
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_GT(0,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}


TEST_F(TfsInit,04_write_null_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 count=0;
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->write(fd, NULL,count);
	 EXPECT_GT(0,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}


TEST_F(TfsInit,05_write_wrong_fd)
{
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=-1;
	 int64_t ret;
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,06_write_not_open_fd)
{
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=56;
	 int64_t ret;
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}


TEST_F(TfsInit,07_open_large_write)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 static char buf[1<<30];
	 
	 
	 int fd;
	 int64_t ret;
	 

	 count=1<<30;
	 
	 suffix=NULL;
	 flags=T_WRITE|T_LARGE;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags,a1Gjpg);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a1Gjpg,buf,0,1<<30);	 
	 ret=tfsclient->write(fd, buf,count);
	 EXPECT_EQ(1<<30,ret);

	tfsclient->close(fd,tfs_name,tfs_name_len);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}

