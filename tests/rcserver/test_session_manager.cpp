/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_session_manager.cpp 524 2011-06-20 07:19:32Z daoan@taobao.com $
 *
 * Authors:
 *   zongdai@taobao.com
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "rcserver/session_manager.h"
#include "rcserver/i_resource_manager.h"
#include "rcserver/resource_server_data.h"
#include "rcserver/mocked_resource_manager.h"
#include "common/session_util.h"
#include "common/define.h"
#include <time.h>
#include <Timer.h>
#include <string>
#include <iostream>

using namespace tfs::rcserver;
using namespace tfs::common;
using namespace tbutil;
using namespace std;

class SessionManagerTest : public ::testing::Test
{
  public:
    SessionManagerTest()
    {
      IResourceManager* resource_mgr_ = new MockedResourceManager();
      resource_mgr_->initialize();

      timer_ = new Timer();
      session_manager_ = new SessionManager(resource_mgr_, timer_);
    }

    ~SessionManagerTest()
    {
    }

    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }

    void gene_info(int sleep_interval);
  private:

  protected:
    SessionManager* session_manager_;
    TimerPtr timer_;
};

void SessionManagerTest::gene_info(int sleep_interval)
{
  ASSERT_EQ(session_manager_->initialize(), TFS_SUCCESS);

  int session_ip = 86420;
  BaseInfo input_info;
  input_info.report_interval_ = 1;
  bool update_flag = false;

  // start
  // app0<->app0_session1 : login(illegal)
  string app0_key = "not_exist_key";
  string app0_session1;
  ASSERT_EQ(session_manager_->login(app0_key, session_ip, app0_session1, input_info), EXIT_APP_NOTEXIST_ERROR);

  // app1<->app1_session1 : login
  string app1_key = "uc";
  string app1_session1;
  ASSERT_EQ(session_manager_->login(app1_key, session_ip, app1_session1, input_info), TFS_SUCCESS);

  // app2<->app2_session1 : login keepalive
  string app2_key = "dc";
  string app2_session1;
  ASSERT_EQ(session_manager_->login(app2_key, session_ip, app2_session1, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app2_1;
  ka_info_app2_1.last_report_time_ = time(NULL) + 10;

  ka_info_app2_1.s_base_info_.session_id_ = app2_session1;
  ka_info_app2_1.s_base_info_.client_version_ = "1.3.1";
  ka_info_app2_1.s_base_info_.cache_size_ = 100000;
  ka_info_app2_1.s_base_info_.cache_time_ = 600;
  ka_info_app2_1.s_base_info_.modify_time_ = 200000;
  ka_info_app2_1.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_2_1;
  app_info_2_1.oper_type_ = OPER_READ;
  app_info_2_1.oper_times_ = 10000;
  app_info_2_1.oper_size_ = 300000;
  app_info_2_1.oper_rt_= 200000;
  app_info_2_1.oper_succ_ = 9997;

  AppOperInfo app_info_2_2;
  app_info_2_2.oper_type_ = OPER_UNLINK;
  app_info_2_2.oper_times_ = 500;
  app_info_2_2.oper_size_ = 60000;
  app_info_2_2.oper_rt_= 20000;
  app_info_2_2.oper_succ_ = 499;

  ka_info_app2_1.s_stat_.app_oper_info_[OPER_READ] = app_info_2_1;
  ka_info_app2_1.s_stat_.app_oper_info_[OPER_UNLINK] = app_info_2_2;
  ka_info_app2_1.s_stat_.cache_hit_ratio_ = 90;
  ASSERT_EQ(session_manager_->keep_alive(app2_session1, ka_info_app2_1, update_flag, input_info), TFS_SUCCESS);

  // app3<->app3_session1 : login keepalive keepalive
  string app3_key = "cc";
  string app3_session1;
  ASSERT_EQ(session_manager_->login(app3_key, session_ip, app3_session1, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app3_1;
  ka_info_app3_1.last_report_time_ = time(NULL) + 12;

  ka_info_app3_1.s_base_info_.session_id_ = app3_session1;
  ka_info_app3_1.s_base_info_.client_version_ = "1.3.1";
  ka_info_app3_1.s_base_info_.cache_size_ = 310000;
  ka_info_app3_1.s_base_info_.cache_time_ = 700;
  ka_info_app3_1.s_base_info_.modify_time_ = 100000;
  ka_info_app3_1.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_3_1;
  app_info_3_1.oper_type_ = OPER_READ;
  app_info_3_1.oper_times_ = 20000;
  app_info_3_1.oper_size_ = 600000;
  app_info_3_1.oper_rt_= 400000;
  app_info_3_1.oper_succ_ = 19000;

  AppOperInfo app_info_3_2;
  app_info_3_2.oper_type_ = OPER_WRITE;
  app_info_3_2.oper_times_ = 1000;
  app_info_3_2.oper_size_ = 50000;
  app_info_3_2.oper_rt_= 10000;
  app_info_3_2.oper_succ_ = 999;

  ka_info_app3_1.s_stat_.app_oper_info_[OPER_READ] = app_info_3_1;
  ka_info_app3_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_3_2;
  ka_info_app3_1.s_stat_.cache_hit_ratio_ = 91;

  ASSERT_EQ(session_manager_->keep_alive(app3_session1, ka_info_app3_1, update_flag, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app3_2;
  ka_info_app3_2.last_report_time_ = time(NULL) + 18;

  ka_info_app3_2.s_base_info_.session_id_ = app3_session1;
  ka_info_app3_2.s_base_info_.client_version_ = "1.3.2";
  ka_info_app3_2.s_base_info_.cache_size_ = 320000;
  ka_info_app3_2.s_base_info_.cache_time_ = 800;
  ka_info_app3_2.s_base_info_.modify_time_ = 110000;
  ka_info_app3_2.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_3_3;
  app_info_3_3.oper_type_ = OPER_UNIQUE_WRITE;
  app_info_3_3.oper_times_ = 1000;
  app_info_3_3.oper_size_ = 50000;
  app_info_3_3.oper_rt_= 10000;
  app_info_3_3.oper_succ_ = 999;

  AppOperInfo app_info_3_4;
  app_info_3_4.oper_type_ = OPER_READ;
  app_info_3_4.oper_times_ = 15000;
  app_info_3_4.oper_size_ = 500000;
  app_info_3_4.oper_rt_= 300000;
  app_info_3_4.oper_succ_ = 15000;

  ka_info_app3_2.s_stat_.app_oper_info_[OPER_READ] = app_info_3_4;
  ka_info_app3_2.s_stat_.app_oper_info_[OPER_UNIQUE_WRITE] = app_info_3_3;
  ka_info_app3_2.s_stat_.cache_hit_ratio_ = 89;

  ASSERT_EQ(session_manager_->keep_alive(app3_session1, ka_info_app3_2, update_flag, input_info), TFS_SUCCESS);


  // app4<->app4_session1 : login keepalive keepalive logout
  string app4_key = "fc";
  string app4_session1;
  ASSERT_EQ(session_manager_->login(app4_key, session_ip, app4_session1, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app4_1;
  ka_info_app4_1.last_report_time_ = time(NULL) + 20;

  ka_info_app4_1.s_base_info_.session_id_ = app3_session1;
  ka_info_app4_1.s_base_info_.client_version_ = "1.3.2";
  ka_info_app4_1.s_base_info_.cache_size_ = 200000;
  ka_info_app4_1.s_base_info_.cache_time_ = 800;
  ka_info_app4_1.s_base_info_.modify_time_ = 100000;
  ka_info_app4_1.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_4_1;
  app_info_4_1.oper_type_ = OPER_UNLINK;
  app_info_4_1.oper_times_ = 1000;
  app_info_4_1.oper_size_ = 6000;
  app_info_4_1.oper_rt_= 40000;
  app_info_4_1.oper_succ_ = 1000;

  AppOperInfo app_info_4_2;
  app_info_4_2.oper_type_ = OPER_WRITE;
  app_info_4_2.oper_times_ = 2000;
  app_info_4_2.oper_size_ = 100000;
  app_info_4_2.oper_rt_= 20000;
  app_info_4_2.oper_succ_ = 1999;

  ka_info_app4_1.s_stat_.app_oper_info_[OPER_UNLINK] = app_info_4_1;
  ka_info_app4_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_4_2;
  ka_info_app4_1.s_stat_.cache_hit_ratio_ = 95;

  ASSERT_EQ(session_manager_->keep_alive(app4_session1, ka_info_app4_1, update_flag, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app4_2;
  ka_info_app4_2.last_report_time_ = time(NULL) + 25;

  ka_info_app4_2.s_base_info_.session_id_ = app3_session1;
  ka_info_app4_2.s_base_info_.client_version_ = "1.3.2";
  ka_info_app4_2.s_base_info_.cache_size_ = 320000;
  ka_info_app4_2.s_base_info_.cache_time_ = 800;
  ka_info_app4_2.s_base_info_.modify_time_ = 110000;
  ka_info_app4_2.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_4_3;
  app_info_4_3.oper_type_ = OPER_UNIQUE_WRITE;
  app_info_4_3.oper_times_ = 6000;
  app_info_4_3.oper_size_ = 90000;
  app_info_4_3.oper_rt_= 30000;
  app_info_4_3.oper_succ_ = 5999;

  AppOperInfo app_info_4_4;
  app_info_4_4.oper_type_ = OPER_READ;
  app_info_4_4.oper_times_ = 25000;
  app_info_4_4.oper_size_ = 900000;
  app_info_4_4.oper_rt_= 500000;
  app_info_4_4.oper_succ_ = 25000;

  ka_info_app4_2.s_stat_.app_oper_info_[OPER_READ] = app_info_4_4;
  ka_info_app4_2.s_stat_.app_oper_info_[OPER_UNIQUE_WRITE] = app_info_4_3;
  ka_info_app4_2.s_stat_.cache_hit_ratio_ = 98;

  ASSERT_EQ(session_manager_->keep_alive(app4_session1, ka_info_app4_2, update_flag, input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app4_3;
  ka_info_app4_3.last_report_time_ = time(NULL) + 30;

  ka_info_app4_3.s_base_info_.session_id_ = app3_session1;
  ka_info_app4_3.s_base_info_.client_version_ = "1.3.2";
  ka_info_app4_3.s_base_info_.cache_size_ = 420000;
  ka_info_app4_3.s_base_info_.cache_time_ = 600;
  ka_info_app4_3.s_base_info_.modify_time_ = 120000;
  ka_info_app4_3.s_base_info_.is_logout_ = true;

  AppOperInfo app_info_4_5;
  app_info_4_5.oper_type_ = OPER_UNIQUE_WRITE;
  app_info_4_5.oper_times_ = 6000;
  app_info_4_5.oper_size_ = 90000;
  app_info_4_5.oper_rt_= 30000;
  app_info_4_5.oper_succ_ = 5999;

  ka_info_app4_3.s_stat_.app_oper_info_[OPER_UNIQUE_WRITE] = app_info_4_5;
  ka_info_app4_3.s_stat_.cache_hit_ratio_ = 97;

  ASSERT_EQ(session_manager_->logout(app4_session1, ka_info_app4_3), TFS_SUCCESS);
  // app5<->app5_session1 : keepalive
  // Todo

  // app6<->app6_session1 : login
  string app6_key = "gc";
  int64_t session_ip_6 = 9000007;
  string app6_session1;
  BaseInfo base_input_info;
  base_input_info.report_interval_ = 1;
  ASSERT_EQ(session_manager_->login(app6_key, session_ip_6, app6_session1, base_input_info), TFS_SUCCESS);
  // app6<->app6_session2 : login keepalive
  string app6_session2;
  ASSERT_EQ(session_manager_->login(app6_key, session_ip_6, app6_session2, base_input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app6_1;
  ka_info_app6_1.last_report_time_ = time(NULL) + 100;

  ka_info_app6_1.s_base_info_.session_id_ = app6_session2;
  ka_info_app6_1.s_base_info_.client_version_ = "2.1.0";
  ka_info_app6_1.s_base_info_.cache_size_ = 200000;
  ka_info_app6_1.s_base_info_.cache_time_ = 1200;
  ka_info_app6_1.s_base_info_.modify_time_ = 100000;
  ka_info_app6_1.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_6_2_1;
  app_info_6_2_1.oper_type_ = OPER_UNIQUE_WRITE;
  app_info_6_2_1.oper_times_ = 6000;
  app_info_6_2_1.oper_size_ = 90000;
  app_info_6_2_1.oper_rt_= 30000;
  app_info_6_2_1.oper_succ_ = 5999;

  AppOperInfo app_info_6_2_2;
  app_info_6_2_2.oper_type_ = OPER_READ;
  app_info_6_2_2.oper_times_ = 24000;
  app_info_6_2_2.oper_size_ = 800000;
  app_info_6_2_2.oper_rt_= 400000;
  app_info_6_2_2.oper_succ_ = 23000;

  ka_info_app6_1.s_stat_.app_oper_info_[OPER_UNIQUE_WRITE] = app_info_6_2_1;
  ka_info_app6_1.s_stat_.app_oper_info_[OPER_READ] = app_info_6_2_2;
  ka_info_app6_1.s_stat_.cache_hit_ratio_ = 95;

  ASSERT_EQ(session_manager_->keep_alive(app6_session2, ka_info_app6_1, update_flag, base_input_info), TFS_SUCCESS);

  // app6<->app6_session3 : keepalive keepalive
  string app6_session3 = "10007-8943231-542143-54364-65375";

  KeepAliveInfo ka_info_app6_2;
  ka_info_app6_2.last_report_time_ = time(NULL) + 120;

  ka_info_app6_2.s_base_info_.session_id_ = app6_session3;
  ka_info_app6_2.s_base_info_.client_version_ = "2.0.2";
  ka_info_app6_2.s_base_info_.cache_size_ = 100000;
  ka_info_app6_2.s_base_info_.cache_time_ = 1000;
  ka_info_app6_2.s_base_info_.modify_time_ = 120000;
  ka_info_app6_2.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_6_3_1;
  app_info_6_3_1.oper_type_ = OPER_WRITE;
  app_info_6_3_1.oper_times_ = 4000;
  app_info_6_3_1.oper_size_ = 80000;
  app_info_6_3_1.oper_rt_= 20000;
  app_info_6_3_1.oper_succ_ = 3999;

  AppOperInfo app_info_6_3_2;
  app_info_6_3_2.oper_type_ = OPER_READ;
  app_info_6_3_2.oper_times_ = 23000;
  app_info_6_3_2.oper_size_ = 800000;
  app_info_6_3_2.oper_rt_= 400000;
  app_info_6_3_2.oper_succ_ = 23000;

  ka_info_app6_2.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_3_1;
  ka_info_app6_2.s_stat_.app_oper_info_[OPER_READ] = app_info_6_3_2;
  ka_info_app6_2.s_stat_.cache_hit_ratio_ = 96;

  ASSERT_EQ(session_manager_->keep_alive(app6_session3, ka_info_app6_2, update_flag, base_input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app6_3;
  ka_info_app6_3.last_report_time_ = time(NULL) + 200;

  ka_info_app6_3.s_base_info_.session_id_ = app6_session3;
  ka_info_app6_3.s_base_info_.client_version_ = "2.0.2";
  ka_info_app6_3.s_base_info_.cache_size_ = 100000;
  ka_info_app6_3.s_base_info_.cache_time_ = 1000;
  ka_info_app6_3.s_base_info_.modify_time_ = 120000;
  ka_info_app6_3.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_6_3_3;
  app_info_6_3_3.oper_type_ = OPER_WRITE;
  app_info_6_3_3.oper_times_ = 14000;
  app_info_6_3_3.oper_size_ = 80000;
  app_info_6_3_3.oper_rt_= 20000;
  app_info_6_3_3.oper_succ_ = 13999;

  ka_info_app6_3.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_3_3;
  ka_info_app6_3.s_stat_.cache_hit_ratio_ = 97;
  ASSERT_EQ(session_manager_->keep_alive(app6_session3, ka_info_app6_3, update_flag, base_input_info), TFS_SUCCESS);
  // app6<->app6_session4 : keepalive logout
   string app6_session4 = "10007-89479900-542143-54364-65375";

  KeepAliveInfo ka_info_app6_4_1;
  ka_info_app6_4_1.last_report_time_ = time(NULL) + 320;

  ka_info_app6_4_1.s_base_info_.session_id_ = app6_session4;
  ka_info_app6_4_1.s_base_info_.client_version_ = "2.1.2";
  ka_info_app6_4_1.s_base_info_.cache_size_ = 200000;
  ka_info_app6_4_1.s_base_info_.cache_time_ = 2000;
  ka_info_app6_4_1.s_base_info_.modify_time_ = 220000;
  ka_info_app6_4_1.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_6_4_1;
  app_info_6_4_1.oper_type_ = OPER_WRITE;
  app_info_6_4_1.oper_times_ = 4000;
  app_info_6_4_1.oper_size_ = 80000;
  app_info_6_4_1.oper_rt_= 20000;
  app_info_6_4_1.oper_succ_ = 3999;

  AppOperInfo app_info_6_4_2;
  app_info_6_4_2.oper_type_ = OPER_READ;
  app_info_6_4_2.oper_times_ = 23000;
  app_info_6_4_2.oper_size_ = 800000;
  app_info_6_4_2.oper_rt_= 400000;
  app_info_6_4_2.oper_succ_ = 23000;

  ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_4_1;
  ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_READ] = app_info_6_4_2;
  ka_info_app6_4_1.s_stat_.cache_hit_ratio_ = 95;

  ASSERT_EQ(session_manager_->keep_alive(app6_session4, ka_info_app6_4_1, update_flag, base_input_info), TFS_SUCCESS);

  KeepAliveInfo ka_info_app6_4_2;
  ka_info_app6_4_2.last_report_time_ = time(NULL) + 200;

  ka_info_app6_4_2.s_base_info_.session_id_ = app6_session3;
  ka_info_app6_4_2.s_base_info_.client_version_ = "2.0.2";
  ka_info_app6_4_2.s_base_info_.cache_size_ = 100000;
  ka_info_app6_4_2.s_base_info_.cache_time_ = 1000;
  ka_info_app6_4_2.s_base_info_.modify_time_ = 120000;
  ka_info_app6_4_2.s_base_info_.is_logout_ = false;

  AppOperInfo app_info_6_4_3;
  app_info_6_4_3.oper_type_ = OPER_WRITE;
  app_info_6_4_3.oper_times_ = 14000;
  app_info_6_4_3.oper_size_ = 80000;
  app_info_6_4_3.oper_rt_= 20000;
  app_info_6_4_3.oper_succ_ = 13999;

  ka_info_app6_4_2.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_4_3;
  ka_info_app6_4_2.s_stat_.cache_hit_ratio_ = 93;
  ASSERT_EQ(session_manager_->logout(app6_session3, ka_info_app6_4_2), TFS_SUCCESS);
  // app6<->app6_session4 : login keepalive keepalive logout

  cout << "first sleep " << sleep_interval << "s" << endl;
  sleep(sleep_interval);
  cout << "end first sleep " << sleep_interval << "s" << endl;
  // app3<->app3_session1 : keepalive logout
  {
    KeepAliveInfo ka_info_app3_1;
    ka_info_app3_1.last_report_time_ = 15000;

    ka_info_app3_1.s_base_info_.session_id_ = app3_session1;
    ka_info_app3_1.s_base_info_.client_version_ = "1.3.1";
    ka_info_app3_1.s_base_info_.cache_size_ = 310000;
    ka_info_app3_1.s_base_info_.cache_time_ = 700;
    ka_info_app3_1.s_base_info_.modify_time_ = 100000;
    ka_info_app3_1.s_base_info_.is_logout_ = false;

    AppOperInfo app_info_3_1;
    app_info_3_1.oper_type_ = OPER_READ;
    app_info_3_1.oper_times_ = 20000;
    app_info_3_1.oper_size_ = 600000;
    app_info_3_1.oper_rt_= 400000;
    app_info_3_1.oper_succ_ = 19000;

    AppOperInfo app_info_3_2;
    app_info_3_2.oper_type_ = OPER_WRITE;
    app_info_3_2.oper_times_ = 1000;
    app_info_3_2.oper_size_ = 50000;
    app_info_3_2.oper_rt_= 10000;
    app_info_3_2.oper_succ_ = 999;

    ka_info_app3_1.s_stat_.app_oper_info_[OPER_READ] = app_info_3_1;
    ka_info_app3_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_3_2;
    ka_info_app3_1.s_stat_.cache_hit_ratio_ = 91;

    ASSERT_EQ(session_manager_->keep_alive(app3_session1, ka_info_app3_1, update_flag, input_info), TFS_SUCCESS);

    KeepAliveInfo ka_info_app3_2;
    ka_info_app3_2.last_report_time_ = 16000;

    ka_info_app3_2.s_base_info_.session_id_ = app3_session1;
    ka_info_app3_2.s_base_info_.client_version_ = "1.3.2";
    ka_info_app3_2.s_base_info_.cache_size_ = 320000;
    ka_info_app3_2.s_base_info_.cache_time_ = 800;
    ka_info_app3_2.s_base_info_.modify_time_ = 110000;
    ka_info_app3_2.s_base_info_.is_logout_ = false;

    AppOperInfo app_info_3_3;
    app_info_3_3.oper_type_ = OPER_UNIQUE_WRITE;
    app_info_3_3.oper_times_ = 1000;
    app_info_3_3.oper_size_ = 50000;
    app_info_3_3.oper_rt_= 10000;
    app_info_3_3.oper_succ_ = 999;

    AppOperInfo app_info_3_4;
    app_info_3_4.oper_type_ = OPER_READ;
    app_info_3_4.oper_times_ = 15000;
    app_info_3_4.oper_size_ = 500000;
    app_info_3_4.oper_rt_= 300000;
    app_info_3_4.oper_succ_ = 15000;

    ka_info_app3_2.s_stat_.app_oper_info_[OPER_READ] = app_info_3_4;
    ka_info_app3_2.s_stat_.app_oper_info_[OPER_UNIQUE_WRITE] = app_info_3_3;
    ka_info_app3_2.s_stat_.cache_hit_ratio_ = 89;

    ASSERT_EQ(session_manager_->logout(app3_session1, ka_info_app3_2), TFS_SUCCESS);
  }

  // app6<->app6_session7 : keepalive

  {
    KeepAliveInfo ka_info_app6_4_1;
    ka_info_app6_4_1.last_report_time_ = time(NULL) + 320;

    ka_info_app6_4_1.s_base_info_.session_id_ = app6_session4;
    ka_info_app6_4_1.s_base_info_.client_version_ = "2.1.2";
    ka_info_app6_4_1.s_base_info_.cache_size_ = 200000;
    ka_info_app6_4_1.s_base_info_.cache_time_ = 2000;
    ka_info_app6_4_1.s_base_info_.modify_time_ = 220000;
    ka_info_app6_4_1.s_base_info_.is_logout_ = false;

    AppOperInfo app_info_6_4_1;
    app_info_6_4_1.oper_type_ = OPER_WRITE;
    app_info_6_4_1.oper_times_ = 4000;
    app_info_6_4_1.oper_size_ = 80000;
    app_info_6_4_1.oper_rt_= 20000;
    app_info_6_4_1.oper_succ_ = 3999;

    AppOperInfo app_info_6_4_2;
    app_info_6_4_2.oper_type_ = OPER_READ;
    app_info_6_4_2.oper_times_ = 23000;
    app_info_6_4_2.oper_size_ = 800000;
    app_info_6_4_2.oper_rt_= 400000;
    app_info_6_4_2.oper_succ_ = 23000;

    ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_4_1;
    ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_READ] = app_info_6_4_2;
    ka_info_app6_4_1.s_stat_.cache_hit_ratio_ = 95;

    ASSERT_EQ(session_manager_->keep_alive(app6_session4, ka_info_app6_4_1, update_flag, base_input_info), TFS_SUCCESS);
  }
  cout << "second sleep " << sleep_interval << "s" << endl;
  sleep(sleep_interval/2);
  {
    KeepAliveInfo ka_info_app6_4_1;
    ka_info_app6_4_1.last_report_time_ = time(NULL) + 320;

    ka_info_app6_4_1.s_base_info_.session_id_ = app6_session4;
    ka_info_app6_4_1.s_base_info_.client_version_ = "2.1.2";
    ka_info_app6_4_1.s_base_info_.cache_size_ = 200000;
    ka_info_app6_4_1.s_base_info_.cache_time_ = 2000;
    ka_info_app6_4_1.s_base_info_.modify_time_ = 220000;
    ka_info_app6_4_1.s_base_info_.is_logout_ = false;

    AppOperInfo app_info_6_4_1;
    app_info_6_4_1.oper_type_ = OPER_WRITE;
    app_info_6_4_1.oper_times_ = 4000;
    app_info_6_4_1.oper_size_ = 80000;
    app_info_6_4_1.oper_rt_= 20000;
    app_info_6_4_1.oper_succ_ = 3999;

    AppOperInfo app_info_6_4_2;
    app_info_6_4_2.oper_type_ = OPER_READ;
    app_info_6_4_2.oper_times_ = 23000;
    app_info_6_4_2.oper_size_ = 800000;
    app_info_6_4_2.oper_rt_= 400000;
    app_info_6_4_2.oper_succ_ = 23000;

    ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_WRITE] = app_info_6_4_1;
    ka_info_app6_4_1.s_stat_.app_oper_info_[OPER_READ] = app_info_6_4_2;
    ka_info_app6_4_1.s_stat_.cache_hit_ratio_ = 95;

    ASSERT_EQ(session_manager_->keep_alive(app6_session4, ka_info_app6_4_1, update_flag, base_input_info), TFS_SUCCESS);
  }
  sleep(sleep_interval/2);
  cout << "end second sleep " << sleep_interval << "s" << endl;
  session_manager_->stop();
}

TEST_F(SessionManagerTest, testSessionStat)
{
  //sleep(22);
  TBSYS_LOG(INFO, "start testSessionStat");
  gene_info(22);
  TBSYS_LOG(INFO, "end testSessionStat");
}

TEST_F(SessionManagerTest, testSessionMonitor)
{
  TBSYS_LOG(INFO, "start testSessionMonitor");
  gene_info(6);
  TBSYS_LOG(INFO, "end testSessionMonitor");
}

TEST_F(SessionManagerTest, testKeepAliveInfo)
{
  KeepAliveInfo ka_info_1;
  ka_info_1.last_report_time_ = 1;
  ka_info_1.s_base_info_.session_id_ = "324-13214-4432-43432-5466";
  ka_info_1.s_base_info_.client_version_ = "1.3.1";
  ka_info_1.s_base_info_.cache_size_ = 1000000;
  ka_info_1.s_base_info_.cache_time_ = 600;
  ka_info_1.s_base_info_.modify_time_ = 30000;
  ka_info_1.s_base_info_.is_logout_ = false;
  ka_info_1.s_stat_.cache_hit_ratio_ = 1000000;

  AppOperInfo app_info_1;
  app_info_1.oper_type_ = OPER_READ;
  app_info_1.oper_times_ = 10000;
  app_info_1.oper_size_ = 300000;
  app_info_1.oper_rt_= 200000;
  app_info_1.oper_succ_ = 9997;

  AppOperInfo app_info_2;
  app_info_2.oper_type_ = OPER_UNLINK;
  app_info_2.oper_times_ = 500;
  app_info_2.oper_size_ = 60000;
  app_info_2.oper_rt_= 20000;
  app_info_2.oper_succ_ = 499;

  ka_info_1.s_stat_.app_oper_info_[OPER_READ] = app_info_1;
  ka_info_1.s_stat_.app_oper_info_[OPER_UNLINK] = app_info_2;

  KeepAliveInfo ka_info_2;
  ka_info_2.last_report_time_ = 2;
  ka_info_2.s_base_info_.session_id_ = "324-13214-4432-43432-5466";
  ka_info_2.s_base_info_.client_version_ = "1.3.3";
  ka_info_2.s_base_info_.cache_size_ = 1000000;
  ka_info_2.s_base_info_.cache_time_ = 600;
  ka_info_2.s_base_info_.modify_time_ = 30000;
  ka_info_2.s_base_info_.is_logout_ = true;
  ka_info_2.s_stat_.cache_hit_ratio_ = 2000000;

  AppOperInfo app_info_3;
  app_info_3.oper_type_ = OPER_READ;
  app_info_3.oper_times_ = 20000;
  app_info_3.oper_size_ = 450000;
  app_info_3.oper_rt_= 380000;
  app_info_3.oper_succ_ = 19999;

  AppOperInfo app_info_4;
  app_info_4.oper_type_ = OPER_WRITE;
  app_info_4.oper_times_ = 400;
  app_info_4.oper_size_ = 70000;
  app_info_4.oper_rt_= 10000;
  app_info_4.oper_succ_ = 400;

  ka_info_2.s_stat_.app_oper_info_[OPER_READ] = app_info_3;
  ka_info_2.s_stat_.app_oper_info_[OPER_WRITE] = app_info_4;

  ka_info_1 += ka_info_2;
  ASSERT_EQ(ka_info_1.s_base_info_.session_id_, "324-13214-4432-43432-5466");
  ASSERT_EQ(ka_info_1.s_base_info_.client_version_, "1.3.3");
  ASSERT_EQ(ka_info_1.s_base_info_.cache_size_, 1000000);
  ASSERT_EQ(ka_info_1.s_base_info_.cache_time_, 600);
  ASSERT_EQ(ka_info_1.s_base_info_.modify_time_, 30000);
  ASSERT_EQ(ka_info_1.s_base_info_.is_logout_, true);

  ASSERT_EQ(ka_info_1.s_stat_.cache_hit_ratio_, 2000000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_.size(), static_cast<size_t>(3));

  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_READ].oper_times_, 10000 + 20000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_READ].oper_size_, 300000 + 450000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_READ].oper_rt_, 200000 + 380000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_READ].oper_succ_, 9997 + 19999);

  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_WRITE].oper_times_, 400);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_WRITE].oper_size_, 70000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_WRITE].oper_rt_, 10000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_WRITE].oper_succ_, 400);

  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_UNLINK].oper_times_, 500);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_UNLINK].oper_size_, 60000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_UNLINK].oper_rt_, 20000);
  ASSERT_EQ(ka_info_1.s_stat_.app_oper_info_[OPER_UNLINK].oper_succ_, 499);
}

TEST_F(SessionManagerTest, testInit)
{
  // test invalid argu
  SessionManager* inval_rc_session_manager = new SessionManager(NULL, timer_);
  ASSERT_EQ(inval_rc_session_manager->initialize(), EXIT_INVALID_ARGU);
  delete inval_rc_session_manager;
  inval_rc_session_manager = NULL;

  IResourceManager* resource_mgr = new MockedResourceManager();
  SessionManager* inval_timer_session_manager = new SessionManager(resource_mgr, 0);
  ASSERT_EQ(inval_timer_session_manager->initialize(), EXIT_INVALID_ARGU);
  delete inval_timer_session_manager;
  delete resource_mgr;
  inval_timer_session_manager = NULL;
  resource_mgr = NULL;

  // init == false, reload_flag = true;
  ASSERT_EQ(session_manager_->initialize(true), EXIT_INVALID_ARGU);
  // init == false, reload_flag = false;
  ASSERT_EQ(session_manager_->initialize(false), TFS_SUCCESS);
  // init == true, reload_flag = false;
  ASSERT_EQ(session_manager_->initialize(false), EXIT_INVALID_ARGU);
  // init == true, reload_flag = true;
  ASSERT_EQ(session_manager_->initialize(true), TFS_SUCCESS);
}

TEST_F(SessionManagerTest, testLogin)
{
  ASSERT_EQ(session_manager_->initialize(), TFS_SUCCESS);

  BaseInfo input_info;
  input_info.report_interval_ = 1;

  // not exist test
  string session_id_1;
  string app_key_1="test_not_exist";
  int64_t session_ip_1 = 600001;
  ASSERT_EQ(session_manager_->login(app_key_1, session_ip_1, session_id_1, input_info), EXIT_APP_NOTEXIST_ERROR);

  // exist test
  string session_id_2;
  string app_key_2="pic";
  int64_t session_ip_2 = 600002;
  ASSERT_EQ(session_manager_->login(app_key_2, session_ip_2, session_id_2, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 10);
  ASSERT_EQ(session_id_2.substr(0,5), "10001");
  ASSERT_EQ(session_id_2.substr(6,6), "600002");

  string session_id_3;
  string app_key_3="dc";
  int64_t session_ip_3 = 600003;
  ASSERT_EQ(session_manager_->login(app_key_3, session_ip_3, session_id_3, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 10);
  ASSERT_EQ(session_id_3.substr(0,5), "10004");
  ASSERT_EQ(session_id_3.substr(6,6), "600003");
}

TEST_F(SessionManagerTest, testKeepAlive)
{
  //ignore test keepalive info now
  ASSERT_EQ(session_manager_->initialize(), TFS_SUCCESS);

  KeepAliveInfo ka_info;
  bool update_flag = false;
  BaseInfo input_info;
  input_info.report_interval_ = 1;

  // not exist app_id
  string session_id_1 = "80000-0876343-4379-342-54";
  ASSERT_EQ(session_manager_->keep_alive(session_id_1, ka_info, update_flag, input_info), EXIT_APP_NOTEXIST_ERROR);

  // invalid session_id: app_id
  string session_id_2 = "8000008763";
  ASSERT_EQ(session_manager_->keep_alive(session_id_2, ka_info, update_flag, input_info), EXIT_SESSIONID_INVALID_ERROR);

  // invalid session_id: no_ip
  string session_id_3 = "8000008764-101";
  ASSERT_EQ(session_manager_->keep_alive(session_id_3, ka_info, update_flag, input_info), EXIT_SESSIONID_INVALID_ERROR);

  string session_id_4 = "10001-846324-432420324-42342-23";
  ka_info.s_base_info_.modify_time_ = 100000;
  ASSERT_EQ(session_manager_->keep_alive(session_id_4, ka_info, update_flag, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 1);
  ASSERT_EQ(update_flag, false);

  ka_info.s_base_info_.modify_time_ = 200000;
  ASSERT_EQ(session_manager_->keep_alive(session_id_4, ka_info, update_flag, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 1);
  ASSERT_EQ(update_flag, false);

  ka_info.s_base_info_.modify_time_ = 90000;
  ASSERT_EQ(session_manager_->keep_alive(session_id_4, ka_info, update_flag, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 20);
  ASSERT_EQ(update_flag, true);

  ka_info.s_base_info_.modify_time_ = 200000;
  ASSERT_EQ(session_manager_->keep_alive(session_id_4, ka_info, update_flag, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 20);
  ASSERT_EQ(update_flag, false);
}

TEST_F(SessionManagerTest, testLogout)
{
  //ignore test keepalive info now
  ASSERT_EQ(session_manager_->initialize(), TFS_SUCCESS);

  KeepAliveInfo ka_info;
  bool update_flag = false;
  BaseInfo input_info;
  input_info.report_interval_ = 1;

  string session_id_1 = "10001-846324-432420324-42342-23";
  ka_info.s_base_info_.modify_time_ = 100000;
  ASSERT_EQ(session_manager_->keep_alive(session_id_1, ka_info, update_flag, input_info), TFS_SUCCESS);
  ASSERT_EQ(input_info.report_interval_, 1);
  ASSERT_EQ(update_flag, false);

  ka_info.s_base_info_.modify_time_ = 200000;
  ASSERT_EQ(session_manager_->logout(session_id_1, ka_info), TFS_SUCCESS);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
