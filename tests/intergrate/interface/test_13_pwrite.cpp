#include"tfs_client_init.h"



TEST_F(TfsInit,01_pwrite_right)
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
	 
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 int64_t offset=0;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(100*(1<<10),ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,02_pwrite_with_count)
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

	 count=50*(1<<10);
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 int64_t offset=0;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(50*(1<<10),ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,03_pwrite_wrong_count)
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
	 int64_t offset=0;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_GT(0,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}


TEST_F(TfsInit,04_pwrite_null_buf)
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
	 int64_t offset=0;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pwrite(fd, NULL,count,offset);
	 EXPECT_GT(0,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}



TEST_F(TfsInit,05_pwrite_wrong_fd)
{
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=-1;
	 int64_t offset=0;
	 int64_t ret;
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,06_pwrite_not_open_fd)
{
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=56;
	 int64_t offset=0;
	 int64_t ret;
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,07_open_large_pwrite)
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
	 int64_t offset=0;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags,a1Gjpg);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a1Gjpg,buf,0,1<<30);	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(1<<30,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,08_pwrite_right_with_offset)
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

	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 int64_t offset=10*(1<<10);
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(100*(1<<10),ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,09_pwrite_right_more_offset)
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
	 
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 int64_t offset=110*(1<<10);
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_EQ(100*(1<<10),ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}

TEST_F(TfsInit,10_pwrite_right_wrong_offset)
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
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_WRITE;
	 int64_t offset=-1;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
     data_in(a100K,buf,0,100*(1<<10));	 
	 ret=tfsclient->pwrite(fd, buf,count,offset);
	 EXPECT_GT(0,ret);

	 tfsclient->close(fd,tfs_name,tfs_name_len);
}


int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}















