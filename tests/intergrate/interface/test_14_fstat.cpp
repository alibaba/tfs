#include"tfs_client_init.h"



TEST_F(TfsInit,01_fstat)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 Ret=tfsclient->fstat(fd,&stat);
	 ASSERT_NE(TFS_SUCCESS,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_fstat_force)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 Ret=tfsclient->fstat(fd,&stat,FORCE_STAT);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_fstat_wrong_fd)
{
	 int fd=-1;
	 int Ret;
	 TfsFileStat stat;
	 
	 Ret=tfsclient->fstat(fd,&stat);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,Ret);

}

TEST_F(TfsInit,04_fstat_not_open)
{
	 int fd=37;
	 int Ret;
	 TfsFileStat stat;
	 
	 Ret=tfsclient->fstat(fd,&stat);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,Ret);

}

TEST_F(TfsInit,05_fstat_NUll_stat)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 Ret=tfsclient->fstat(fd,NULL);
	 ASSERT_EQ(TFS_ERROR,Ret);
	 
	 Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_EQ(TFS_SUCCESS,Ret);
	 
	 tfsclient->close(fd);
}


int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}





