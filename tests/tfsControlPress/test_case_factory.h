/*
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_case_factory.h 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#ifndef TEST_CASE_FACTORY_H
#define TEST_CASE_FACTORY_H

#include <string>

class TestTfsCase;

class TestCaseFactory 
{
  public:
    static TestTfsCase *getTestCase(string testIndex);
    
};

#endif
