/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rc_api_cases.cpp 575 2011-07-18 06:51:44Z daoan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "rc_api_cases.h"
#include <iostream>
#include <vector>
#include "common/func.h"
#include "common/rc_define.h"
#include "common/client_manager.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

vector<string> RcApiTest::authorized_keys_;
vector<string> RcApiTest::not_authorized_keys_;
vector<uint64_t> RcApiTest::app_ids_;
int64_t RcApiTest::rc_ip_;
BasePacketFactory* RcApiTest::packet_factory_ = NULL;
BasePacketStreamer* RcApiTest::packet_streamer_ = NULL;

void RcApiTest::SetUpTestCase()
{
  authorized_keys_.clear();
  authorized_keys_.push_back("shopcenter_key");
  authorized_keys_.push_back("tappkey00001");
  authorized_keys_.push_back("tappkey00002");

  not_authorized_keys_.push_back("tappkey00003");
  not_authorized_keys_.push_back("tappkey00004");
  not_authorized_keys_.push_back("tappkey00005");

  vector< pair<string, int64_t> > app_ips;
  app_ips.push_back(make_pair("10.232.35.40", 12000));
  app_ips.push_back(make_pair("10.232.35.40", 13000));
  app_ips.push_back(make_pair("10.232.35.40", 15000));
  app_ips.push_back(make_pair("10.232.35.41", 8000));
  app_ips.push_back(make_pair("172.24.80.1", 7000));
  app_ips.push_back(make_pair("172.24.80.1", 5000));
  app_ips.push_back(make_pair("172.24.81.1", 17000));
  
  for (size_t i = 0; i < app_ips.size(); ++i)
  {
    app_ids_.push_back(Func::str_to_addr(app_ips[i].first.c_str(), app_ips[i].second));
  }

  srand(time(NULL) + rand() + pthread_self());
}

void RcApiTest::TearDownTestCase()
{
}

void RcApiTest::login(const int num)
{
  int invalid_flag = 0;
  int key_id = 0;
  int app_id = 0;
  for (int i = 0; i < num; ++i)
  {
    invalid_flag = rand() % 2;
    string app_key;
    if (0 == invalid_flag)
    {
      key_id = rand() % authorized_keys_.size();
      app_key = authorized_keys_[key_id];
    }
    else
    {
      key_id = rand() % not_authorized_keys_.size();
      app_key = not_authorized_keys_[key_id];
    }

    app_id = rand() % app_ids_.size();
    string session_id;
    BaseInfo base_info;
    cout << "index: " << i << " invalid_flag: " << invalid_flag << " rc ip: " << rc_ip_ << " app key: " << app_key.c_str() << " app id: "  << app_id << endl;
    if (0 == invalid_flag)
    {
      ASSERT_EQ(TFS_SUCCESS, RcHelper::login(rc_ip_, app_key, app_ids_[app_id], session_id, base_info));
      base_info.dump();
    }
    else
    {
      ASSERT_NE(TFS_SUCCESS, RcHelper::login(rc_ip_, app_key, app_ids_[app_id], session_id, base_info));
      base_info.dump();
    }
  }
}

void RcApiTest::keep_alive(const int num)
{
  UNUSED(num);
}

TEST_F(RcApiTest, test_login)
{
  RcApiTest::login(500);
}

void usage(char* name)
{
  printf("Usage: %s -s rcip:port\n", name);
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char rcip[256];
  int i = 0;

  memset(rcip, 0, 256);
  while ((i = getopt(argc, argv, "s:i")) != EOF)
  {
    switch (i)
    {
      case 's':
        strcpy(rcip, optarg);
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if ('\0' == rcip[0])
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  cout << "rcip: " << rcip << endl;
  RcApiTest::rc_ip_ = Func::get_host_ip(rcip);
  cout << "rc_ip_: " << RcApiTest::rc_ip_ << endl;

  RcApiTest::packet_factory_ = new MessageFactory();
  RcApiTest::packet_streamer_ = new BasePacketStreamer();

  RcApiTest::packet_streamer_->set_packet_factory(RcApiTest::packet_factory_);
  NewClientManager::get_instance().initialize(RcApiTest::packet_factory_, RcApiTest::packet_streamer_);
  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (ret == TFS_ERROR)
  {
    printf("input argument error...\n");
  }
  return RUN_ALL_TESTS();
}
