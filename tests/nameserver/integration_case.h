#ifndef TFS_TESTS_NAMESERVER_INTEGRATION_CASE_H
#define TFS_TESTS_NAMESERVER_INTEGRATION_CASE_H

/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: integration_case.h 2090 2011-03-14 16:11:58Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <tbnet.h>
#include <Timer.h>
#include <ThreadPool.h>
#include <StaticMutex.h>

namespace tfs
{
namespace integration 
{
class IntegrationServiceInstance;
class IntegrationBaseCase : public tbutil::ThreadPoolWorkItem
{
public:
  IntegrationBaseCase(IntegrationServiceInstance* instance, const char* name, int32_t mode_count, int32_t mode = 0);
  virtual ~IntegrationBaseCase();
  virtual void execute(const tbutil::ThreadPool* pool) = 0;
  virtual void destroy() = 0;
  virtual IntegrationBaseCase* new_object(int32_t mode) = 0;
  int32_t mode_count()const { return mode_count_;} 
  virtual std::string& name() { return name_;}
protected:
  IntegrationServiceInstance* instance_;
  int32_t mode_;
  int32_t mode_count_;
  std::string name_;
};

class KickCase: public IntegrationBaseCase
{
public:
  KickCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  ~KickCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};

class OpenCloseCase : public IntegrationBaseCase 
{
public:
  OpenCloseCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  virtual ~OpenCloseCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};

class BuildReplicateCase: public IntegrationBaseCase
{
public:
  BuildReplicateCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  virtual ~BuildReplicateCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};

class BuildBalanceCase: public IntegrationBaseCase
{
public:
  BuildBalanceCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  virtual ~BuildBalanceCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};

class BuildCompactCase: public IntegrationBaseCase
{
public:
  BuildCompactCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  virtual ~BuildCompactCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};

class BuildRedundantCase: public IntegrationBaseCase
{
public:
  BuildRedundantCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode = 0);
  virtual ~BuildRedundantCase();
  virtual void execute(const tbutil::ThreadPool* pool);
  virtual void destroy();
  IntegrationBaseCase* new_object(int32_t mode);
};
}
}

#endif

