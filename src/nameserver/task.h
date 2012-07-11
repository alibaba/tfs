/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMESERVER_TASK_H_
#define TFS_NAMESERVER_TASK_H_

#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include <Timer.h>
#include "gc.h"
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class TaskManager;
    class Task: public GCObject
    {
      friend class TaskManager;
      #ifdef TFS_GTEST
      friend class TaskTest;
      friend class TaskManagerTest;
      friend class LayoutManagerTest;
      FRIEND_TEST(TaskTest, dump);
      FRIEND_TEST(TaskManagerTest, add);
      FRIEND_TEST(LayoutManagerTest, build_emergency_replicate_);
      FRIEND_TEST(LayoutManagerTest, check_emergency_replicate_);
      FRIEND_TEST(LayoutManagerTest, build_replicate_task_);
      FRIEND_TEST(LayoutManagerTest, build_compact_task_);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      FRIEND_TEST(LayoutManagerTest, build_redundant_);
      #endif
      public:
      Task(TaskManager& manager, const common::PlanType type,
          const common::PlanPriority priority, const uint32_t block_id,
          const std::vector<ServerCollect*>& runer);
      virtual ~ Task(){};
      virtual int handle() = 0;
      virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag) = 0;
      virtual void dump(tbnet::DataBuffer& stream);
      virtual void dump(const int32_t level, const char* const format = NULL);
      virtual void runTimerTask();
      bool operator < (const Task& task) const;
      bool need_add_server_to_map() const;
      bool timeout(const time_t now) const;
      protected:
      std::vector<ServerCollect*> runer_;
      uint32_t block_id_;
      common::PlanType type_;
      common::PlanStatus status_;
      common::PlanPriority priority_;
      int64_t seqno_;
      TaskManager& manager_;
      private:
      DISALLOW_COPY_AND_ASSIGN(Task);
    };

    struct TaskCompare
    {
      bool operator()(const Task* lhs, const Task* rhs) const
      {
        return (*lhs) < (*rhs);
      }
    };

    class CompactTask: public Task
    {
      #ifdef TFS_GTEST
      friend class TaskTest;
      FRIEND_TEST(TaskTest, compact_handle);
      FRIEND_TEST(TaskTest, compact_task_do_complete);
      FRIEND_TEST(TaskTest, compact_task_check_complete);
      #endif
      struct CompactComplete
      {
        uint64_t id_;
        uint32_t block_id_;
        common::PlanStatus status_;
        bool all_success_;
        bool has_success_;
        bool is_complete_;
        bool current_complete_result_;
        common::BlockInfo block_info_;
        CompactComplete(const uint64_t id, const uint32_t block_id, const common::PlanStatus status):
          id_(id), block_id_(block_id), status_(status), all_success_(true),
          has_success_(false), is_complete_(true), current_complete_result_(false){}
      };
      public:
      CompactTask(TaskManager& manager, const common::PlanPriority priority,
          const uint32_t block_id, const std::vector<ServerCollect*>& runer);
      virtual ~CompactTask(){}
      virtual int handle();
      virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
      virtual void runTimerTask();
      virtual void dump(tbnet::DataBuffer& stream);
      virtual void dump(const int32_t level, const char* const format = NULL);
      private:
      void check_complete(CompactComplete& value, common::VUINT64& ds_list);
      int do_complete(CompactComplete& value, common::VUINT64& ds_list);
      static common::CompactStatus status_transform_plan_to_compact(const common::PlanStatus status);
      static common::PlanStatus status_transform_compact_to_plan(const common::CompactStatus status);
      private:
      static const int8_t INVALID_SERVER_ID;
      static const int8_t INVALID_BLOCK_ID;
      std::vector< std::pair <uint64_t, common::PlanStatus> > complete_status_;
      common::BlockInfo block_info_;
      tbutil::Mutex mutex_;
      private:
      DISALLOW_COPY_AND_ASSIGN(CompactTask);
    };

    class ReplicateTask : public Task
    {
      #ifdef TFS_GTEST
      friend class TaskTest;
      FRIEND_TEST(TaskTest, replicate_task_handle_complete);
      #endif
      public:
        ReplicateTask(TaskManager& manager, const common::PlanPriority priority,
          const uint32_t block_id, const std::vector<ServerCollect*>& runer);
        virtual ~ReplicateTask(){}
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
      protected:
        common::ReplicateBlockMoveFlag flag_;
      private:
        DISALLOW_COPY_AND_ASSIGN(ReplicateTask);
    };

    class DeleteTask : public Task
    {
      public:
        DeleteTask(TaskManager& manager, const common::PlanPriority priority,
          const uint32_t block_id, const std::vector<ServerCollect*>& runer);
        virtual ~DeleteTask(){}
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
      private:
        DISALLOW_COPY_AND_ASSIGN(DeleteTask);
    };

    class MoveTask : public ReplicateTask
    {
      public:
        MoveTask(TaskManager& manager, const common::PlanPriority priority,
          const uint32_t block_id, const std::vector<ServerCollect*>& runer);
        virtual ~MoveTask(){}
      private:
        DISALLOW_COPY_AND_ASSIGN(MoveTask);
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif

