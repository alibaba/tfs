/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_api.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_tfs_case.h"
#include "test_case_factory.h"
#include "test_tfs_api.h"


class TestTfsCase;
class TestCaseFactory;

bool TestTfsAPI::init(const char *filename)
{
  tbsys::CConfig::getCConfig().load(const_cast<char *> (filename));
  _testThreadCount = tbsys::CConfig::getCConfig().getInt("public", "thread_count");
  _timeInter = tbsys::CConfig::getCConfig().getInt("public", "time_interval");
  setThreadCount(_testThreadCount);
  return true;
}

void TestTfsAPI::setTestIndex(const char *testIndex)
{
  _testIndex = const_cast<char *> (testIndex);
}

void TestTfsAPI::run(tbsys::CThread *thread, void *arg)
{
  TestTfsCase *tcase = TestCaseFactory::getTestCase(_testIndex);
  if (tcase == NULL) {
    return;
  }
  tcase->setUp();
  while (!_stop) {
    tcase->run();
    usleep(_timeInter);
  }
  tcase->tearDown();;
  return;
}

