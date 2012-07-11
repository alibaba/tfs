#include"tfs_init_for_test.h"

TEST_F(TfsInitTest,01_initialize_right)
{    
    int ret;
	
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl;
	EXPECT_EQ(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,02_initialize_null_nsaddr)
{    
    int ret;
	
    const char* ns_addr = NULL; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl;
	EXPECT_EQ(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,03_initialize_empty_nsaddr)
{    
    int ret;
	
    const char* ns_addr = ""; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_EQ(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,04_initialize_wrong_nsaddr)
{    
    int ret;
	
    const char* ns_addr = "10.uyt.278.2:5320"; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,05_initialize_wrong_nsaddr_1)
{    
    int ret;
	
    const char* ns_addr = "1sss.uyt.278.2:5320"; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,06_initialize_wrong_nsaddr_2)
{    
    int ret;
	
    const char* ns_addr = "4164651gjhgjk"; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,07_initialize_not_usable)
{    
    int ret;
	
    const char* ns_addr = "10.232.4.3:5100"; 
	ret=tfsclient->initialize(ns_addr);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,08_initialize_wrong_cachetime_1)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=-1;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);;
	tfsclient->destroy();
}

TEST_F(TfsInitTest,09_initialize_wrong_cachetime_2)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=0;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time);
 cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,10_initialize_small_cachetime)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=5;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time);
 cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,11_initialize_large_cachetime)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=2147483647;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time);
 cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,12_initialize_wrong_cacheitems_1)
{    
    int32_t  cache_time;
    int32_t  cache_items;	
    int ret;
	         
    cache_time=1800;
	cache_items=-1;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time,cache_items);
 cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,13_initialize_wrong_cacheitems_2)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=0;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time,cache_items);
 cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,14_initialize_small_cacheitems)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=5;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time,cache_items);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,15_initialize_large_cacheitems)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=2147483647;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time,cache_items);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_NE(TFS_SUCCESS,ret);
	tfsclient->destroy();
}

TEST_F(TfsInitTest,16_initialize_right)
{    
    int32_t  cache_time; 
    int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
    cache_items=500000;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->initialize(ns_addr,cache_time,cache_items,false);
    cout<<"@@@@@@@"<<ret<<"@@@@@@@@@@@"<<endl; 
	EXPECT_EQ(TFS_SUCCESS,ret);
	tfsclient->destroy();
}
int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}








