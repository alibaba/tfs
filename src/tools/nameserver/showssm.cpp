/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: showssm.cpp 199 2011-04-12 08:49:55Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <ext/hash_map>
#include <signal.h>
#include <Memory.hpp>

#include "common/config.h"
#include "message/client.h"
#include "message/client_pool.h"
#include "message/message.h"
#include "message/message_factory.h"
#include "nameserver/layout_manager.h"
#include "common/config_item.h"

using namespace __gnu_cxx;
using namespace tfs::message;
using namespace tfs::nameserver;
using namespace tfs::common;

static int32_t stop = 0;
static int32_t report = 0;
static int32_t min_replication = 3;
static int32_t max_block_size;
static int32_t max_write_filecount;
static uint32_t group_mask = 0;
static INT_MAP group_map;
static char server_addr[128];
typedef hash_map<uint64_t, DataServerStatInfo*, hash<int32_t> > DATASERVER_MAP;
typedef DATASERVER_MAP::iterator DATASERVER_MAP_ITER;
static DATASERVER_MAP ds_map_;
static const std::string LAST_DS_FILE("%s/.tfs_last_ds");

struct ipaddr_sort
{
  bool operator()(ServerCollect* x, ServerCollect* y) const
  {
    IpAddr* ax = reinterpret_cast<IpAddr*>(&(x->get_ds()->id_));
    IpAddr* ay = reinterpret_cast<IpAddr*>(&(y->get_ds()->id_));
    uint32_t ipx = ((ax->ip_ & 0xFF000000) >> 24) + ((ax->ip_ & 0xFF0000) >> 8);
    uint32_t ipy = ((ay->ip_ & 0xFF000000) >> 24) + ((ay->ip_ & 0xFF0000) >> 8);
    return (ipx < ipy);
  }
};

static void load_last_ds()
{
  char* home = getenv("HOME");
  char path[256];
  sprintf(path, LAST_DS_FILE.c_str(), home ? home : "");
  int32_t fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", path, strerror(errno));
		return;
	}
  int32_t size = 0;
  if (read(fd, reinterpret_cast<char*>(&size), INT_SIZE) != INT_SIZE)
  {
    close(fd);
		TBSYS_LOG(ERROR, "read size fail");
    return;
  }
  for (int32_t i = 0; i < size; i++)
  {
    DataServerStatInfo* ds = new DataServerStatInfo();
    if (read(fd, ds, sizeof(DataServerStatInfo)) != sizeof(DataServerStatInfo))
    {
			tbsys::gDelete(ds);
      close(fd);
      return;
    }
    ds_map_[ds->id_] = ds;
  }
  close(fd);
}

static void save_last_ds()
{
  char* home = getenv("HOME");
  char path[256];
  sprintf(path, LAST_DS_FILE.c_str(), home ? home : "");
  int32_t fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
  {
		TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", path, strerror(errno));
    return;
  }
  int32_t size = ds_map_.size();
  if (write(fd, reinterpret_cast<char*>(&size), INT_SIZE) != INT_SIZE)
  {
    close(fd);
    return;
  }
  for (DATASERVER_MAP_ITER it = ds_map_.begin(); it != ds_map_.end(); it++)
  {
    if (write(fd, it->second, sizeof(DataServerStatInfo)) != sizeof(DataServerStatInfo))
    {
      close(fd);
      return;
    }
  }
  close(fd);
}

void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
      stop = 1;
      break;
  }
}

void compute_tp(Throughput* tp, int32_t time)
{
  if (time < 1)
  {
    return;
  }
  tp->write_byte_ /= time;
  tp->write_file_count_ /= time;
  tp->read_byte_ /= time;
  tp->read_file_count_ /= time;
}

void add_tp(Throughput* tp, Throughput* atp, int32_t sign)
{
  tp->write_byte_ = sign * tp->write_byte_ + atp->write_byte_;
  tp->write_file_count_ = sign * tp->write_file_count_ + atp->write_file_count_;
  tp->read_byte_ = sign * tp->read_byte_ + atp->read_byte_;
  tp->read_file_count_ = sign * tp->read_file_count_ + atp->read_file_count_;
}

inline int get_group_index(const uint64_t ip)
{
  if (0 == group_mask)
  {
    return 0;
  }
  uint32_t lan = Func::get_lan(ip, group_mask);
  INT_MAP_ITER it = group_map.find(lan);
  int32_t index = 0;
  if (it == group_map.end())
  {
    index = group_map.size() + 1;
    group_map.insert(INT_MAP::value_type(lan, index));
  }
  else
  {
    index = it->second;
  }
  return index;
}

inline char* get_addr_string(const uint64_t ip)
{
  int32_t index = get_group_index(ip);
  if (0 == index)
  {
    sprintf(server_addr, "%s", tbsys::CNetUtil::addrToString(ip).c_str());
  }
  else
  {
    char* p = const_cast<char*>(tbsys::CNetUtil::addrToString(ip).c_str());
    if (strlen(p) > 20)
    {
      *(p + 20) = '\0';
    }
    sprintf(server_addr, "%-20s(%d)", p, index);
  }
  return server_addr;
}

void print_block_map(BLOCK_MAP* block_map)
{
  int64_t total_file_count = 0;
  int64_t total_size = 0;
  int64_t total_delfile_count = 0;
  int64_t total_del_size = 0;
  printf("BLOCK_ID   VERSION    FILECOUNT  SIZE       DEL_FILE   DEL_SIZE   SEQ_NO   COPIES\n");
  printf("---------- ---------- ---------- ---------- ---------- ---------- ------   ------\n");
  BLOCK_MAP_ITER it = block_map->begin();
  for (; it != block_map->end(); it++)
  {
    BlockCollect* block_collect = it->second;
    const BlockInfo* block_info = block_collect->get_block_info();
    const VUINT64* server_list = block_collect->get_ds();
    if (!report || block_info->size_ < max_block_size || block_info->size_ > max_block_size * 2)
    {
      fprintf(stdout,"%-10u %10d %10d %10d %10d %10d %5u %5d\n", block_info->block_id_, block_info->version_, block_info->file_count_, block_info->size_,
          block_info->del_file_count_, block_info->del_size_, block_info->seq_no_, static_cast<int32_t>(server_list->size()));
    }
    total_file_count += block_info->file_count_;
    total_size += block_info->size_;
    total_delfile_count += block_info->del_file_count_;
    total_del_size += block_info->del_size_;
  }
  fprintf(stdout,"TOTAL:     %10Zd %10"PRI64_PREFIX"d %10s %10"PRI64_PREFIX"d %10s\n\n",
			block_map->size(),
			total_file_count,
      Func::format_size(total_size).c_str(),
			total_delfile_count,
			Func::format_size(total_del_size).c_str());
}

void print_block_map_ex(BLOCK_MAP* block_map)
{
  int32_t count = 0;
  printf("BLOCK_ID   SERVER_LIST\n");
  printf("---------- -----------------------------------------------------------------------------------------\n");
  for (BLOCK_MAP_ITER it = block_map->begin(); it != block_map->end(); it++)
  {
    BlockCollect* block_collect = it->second;
    const BlockInfo* block_info = block_collect->get_block_info();
    const VUINT64* server = block_collect->get_ds();
    if (report)
    {
      if (static_cast<int32_t>(server->size()) >= min_replication)
      {
        INT_MAP repl_map;
        int32_t hrep = 0;
        for (int32_t i = 0; i < static_cast<int32_t>(server->size()); i++)
        {
          int32_t gi = get_group_index(server->at(i));
          if (gi == 0)
            break;
          if (repl_map.find(gi) != repl_map.end())
          {
            hrep = 1;
            break;
          }
          repl_map[gi] = 1;
        }
        if (!hrep)
        {
          continue;
        }
      }
    }
    printf("%-10u ", block_info->block_id_);
    for (int32_t i = 0; i < static_cast<int32_t>(server->size()); i++)
    {
      printf("%-23s", get_addr_string(server->at(i)));
    }
    if (static_cast<int32_t>(server->size()) < min_replication)
    {
      printf("%-23s", "NEED_REPL");
    }
    printf("\n");
    count++;
  }
  if (0 == count)
  {
    printf("ALL OK\n\n");
  }
}

void print_server_map(SERVER_MAP* ds_map)
{
  int32_t t_load = 0;
  int64_t total_capacity = 1;
  int64_t use_capacity = 1;
  Throughput global_tp;
  Throughput global_ltp;
  DataServerStatInfo* tmp_ds = new DataServerStatInfo();
  memset(&global_tp, 0, sizeof(Throughput));
  memset(&global_ltp, 0, sizeof(Throughput));
  int32_t nowTime = time(NULL);
  vector<ServerCollect*> ds_list;
  for (SERVER_MAP_ITER it = ds_map->begin(); it != ds_map->end(); it++)
  {
    ds_list.push_back(it->second);
  }
  sort(ds_list.begin(), ds_list.end(), ipaddr_sort());
  printf(
    "SERVER_ADDR           UCAP  / TCAP =  UR  BLKCNT LOAD LA TOTAL_WRITE  TOTAL_READ   LAST_WRITE   LAST_READ    STARTUP_TIME\n");
  printf(
    "--------------------- ------------------- ------ ---- -- ------------ ------------ ------------ ------------ -------------------\n");
  int32_t i = 0;
  int32_t list_size = ds_list.size();
  for (i = 0; i <list_size; i++)
  {
    ServerCollect* ser_vcol = ds_list[i];
    DataServerStatInfo* ds = ser_vcol->get_ds();
    memcpy(tmp_ds, ds, sizeof(DataServerStatInfo));
    DATASERVER_MAP_ITER ds_sit = ds_map_.find(ds->id_);
    DataServerStatInfo* old_ds = NULL;
    if (ds_sit == ds_map_.end())
    {
      old_ds = new DataServerStatInfo();
      memcpy(old_ds, ds, sizeof(DataServerStatInfo));
      ds_map_[ds->id_] = old_ds;
    }
    else
    {
      old_ds = ds_sit->second;
    }
    add_tp(&(old_ds->total_tp_), &(tmp_ds->total_tp_), -1); // compute the diff
    compute_tp(&(old_ds->total_tp_), tmp_ds->current_time_ - old_ds->current_time_);
    compute_tp(&(tmp_ds->total_tp_), tmp_ds->current_time_ - tmp_ds->startup_time_);
    add_tp(&global_tp, &(tmp_ds->total_tp_), 1);
    add_tp(&global_ltp, &(old_ds->total_tp_), 1);
    t_load += tmp_ds->current_load_;
    total_capacity += tmp_ds->total_capacity_;
    use_capacity += tmp_ds->use_capacity_;

    fprintf(stdout, "%-21s %7s %7s %2d%% %6d %4d %2d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %-19s\n",
            get_addr_string(tmp_ds->id_),
            Func::format_size(tmp_ds->use_capacity_).c_str(),
            Func::format_size(tmp_ds->total_capacity_).c_str(),
            static_cast<int32_t> (tmp_ds->use_capacity_ * 100 / tmp_ds->total_capacity_),
            tmp_ds->block_count_,
            tmp_ds->current_load_,
            nowTime - tmp_ds->last_update_time_,
            Func::format_size(tmp_ds->total_tp_.write_byte_).c_str(),
            tmp_ds->total_tp_.write_file_count_,
            Func::format_size(tmp_ds->total_tp_.read_byte_).c_str(),
            tmp_ds->total_tp_.read_file_count_,
            Func::format_size(old_ds->total_tp_.write_byte_).c_str(),
            old_ds->total_tp_.write_file_count_,
            Func::format_size(old_ds->total_tp_.read_byte_).c_str(),
            old_ds->total_tp_.read_file_count_,
            Func::time_to_str(tmp_ds->startup_time_).c_str());
    memcpy(old_ds, ds, sizeof(DataServerStatInfo));
  }
  if (ds_map->size() > 0)
  {
    t_load /= ds_map->size();
  }

  fprintf(stdout, "TOTAL %5Zd %9s %7s %7s %2d%% %6s %4d %2s %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d\n\n",
          ds_map->size(),
          "",
          Func::format_size(use_capacity).c_str(),
          Func::format_size(total_capacity).c_str(),
          static_cast<int32_t>(use_capacity * 100 / total_capacity),
          "",
          t_load,
          "",
          Func::format_size(global_tp.write_byte_).c_str(),
          global_tp.write_file_count_,
          Func::format_size(global_tp.read_byte_).c_str(),
          global_tp.read_file_count_,
          Func::format_size(global_ltp.write_byte_).c_str(),
          global_ltp.write_file_count_,
          Func::format_size(global_ltp.read_byte_).c_str(),
          global_ltp.read_file_count_);

  delete tmp_ds;
  tmp_ds = NULL;
}

void print_machine(SERVER_MAP* ds_map, int32_t flag)
{
  char tmp_ipport[64] = {'\0'};
  char tmp_ip[64] = {'\0'};
  char tmp_ip_next[64] = {'\0'};

  DataServerStatInfo* tmp_ds = new DataServerStatInfo();
  vector<ServerCollect*> ds_list;
  for (SERVER_MAP_ITER it = ds_map->begin(); it != ds_map->end(); it++)
  {
    ds_list.push_back(it->second);
  }
  if (0 == ds_list.size())
  {
    return;
  }
  sort(ds_list.begin(), ds_list.end(), ipaddr_sort());
  if (1 == flag)
  {
    printf(
      "SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD TOTAL_WRITE  TOTAL_READ  LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ\n");
    printf(
      "------------- ---- ------------------ -------- ---- -----------  ----------  ----------  ---------  --------  ---------\n");
  }
  else
  {
    printf(
      "SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ STARTUP_TIME\n");
    printf(
      "------------- ---- ------------------ -------- ---- ----------  ---------  ---------  -------- ------------\n");
  }
  int32_t index_ip = 0;
	int32_t total_machine = 0;
  uint64_t use_machine_cap = 0;
	uint64_t total_machine_cap = 0;
  uint64_t use_cluster_cap = 0;
	uint64_t total_cluster_cap = 0;
  uint64_t total_blk_count = 0;
	uint64_t machine_blk_count = 0;
  uint32_t machine_load = 0;
	uint32_t cluster_load = 0;
  int32_t latest_startup_time = 0;
  uint32_t time_interval_tmp = 0;
  uint64_t time_interval_total = 0;

  Throughput machine_tp;
  Throughput old_ds_copy, machine_ltp_total;
  Throughput cluster_tp, cluster_ltp;
  Throughput max_read_ltp, max_write_ltp;
  memset(&machine_tp, 0, sizeof(Throughput));
  memset(&old_ds_copy, 0, sizeof(Throughput));
  memset(&machine_ltp_total, 0, sizeof(Throughput));
  memset(&cluster_tp, 0, sizeof(Throughput));
  memset(&cluster_ltp, 0, sizeof(Throughput));
  memset(&max_read_ltp, 0, sizeof(Throughput));
  memset(&max_write_ltp, 0, sizeof(Throughput));

  for (int32_t i = 0; i < static_cast<int32_t> (ds_list.size()); i++)
  {
    ServerCollect* ser_vcol = ds_list[i];
    DataServerStatInfo* ds = ser_vcol->get_ds();
    memcpy(tmp_ds, ds, sizeof(DataServerStatInfo));

    DATASERVER_MAP_ITER ds_sit = ds_map_.find(ds->id_);
    DataServerStatInfo* old_ds = NULL;
    if (ds_sit == ds_map_.end())
    {
      old_ds = new DataServerStatInfo();
      memcpy(old_ds, ds, sizeof(DataServerStatInfo));
      ds_map_[ds->id_] = old_ds;
    }
    else
    {
      old_ds = ds_sit->second;
    }

    add_tp(&(old_ds->total_tp_), &(tmp_ds->total_tp_), -1); // compute the diff

    memcpy(&old_ds_copy, &(old_ds->total_tp_), sizeof(Throughput));
    time_interval_tmp = tmp_ds->current_time_ - old_ds->current_time_;

    compute_tp(&(old_ds->total_tp_), tmp_ds->current_time_ - old_ds->current_time_);
    compute_tp(&(tmp_ds->total_tp_), tmp_ds->current_time_ - tmp_ds->startup_time_);

    add_tp(&(cluster_tp), &(tmp_ds->total_tp_), 1);
    //  add_tp(&(cluster_ltp), &(old_ds->total_tp_),1);

    use_cluster_cap += tmp_ds->use_capacity_;
    total_cluster_cap += tmp_ds->total_capacity_;
    total_blk_count += tmp_ds->block_count_;

    strncpy(tmp_ipport, tbsys::CNetUtil::addrToString(tmp_ds->id_).c_str(), 32);
    if (0 == strlen(tmp_ip))
    {
      memcpy(&max_read_ltp, &(old_ds->total_tp_), sizeof(Throughput));
      memcpy(&max_write_ltp, &(old_ds->total_tp_), sizeof(Throughput));
      latest_startup_time = tmp_ds->startup_time_;
      strncpy(tmp_ip, tmp_ipport, strchr(tmp_ipport, ':') - tmp_ipport);
      strncpy(tmp_ip_next, tmp_ipport, strchr(tmp_ipport, ':') - tmp_ipport);
    }
    memset(tmp_ip_next, '\0', 64);
    strncpy(tmp_ip_next, tmp_ipport, strchr(tmp_ipport, ':') - tmp_ipport);

    if (0 == strncmp(tmp_ip, tmp_ip_next, strlen(tmp_ip_next)))
    {
      ++index_ip;
      use_machine_cap += tmp_ds->use_capacity_;
      total_machine_cap += tmp_ds->total_capacity_;
      machine_blk_count += tmp_ds->block_count_;
      machine_load = tmp_ds->current_load_;
      add_tp(&(machine_tp), &(tmp_ds->total_tp_), 1);

      if ((max_read_ltp.read_byte_) <= (old_ds->total_tp_.read_byte_))
      {
        max_read_ltp.read_file_count_ = old_ds->total_tp_.read_file_count_;
        max_read_ltp.read_byte_ = old_ds->total_tp_.read_byte_;
      }
      if ((max_write_ltp.write_byte_) <= (old_ds->total_tp_.write_byte_))
      {
        max_write_ltp.write_file_count_ = old_ds->total_tp_.write_file_count_;
        max_write_ltp.write_byte_ = old_ds->total_tp_.write_byte_;
      }
      if (latest_startup_time < tmp_ds->startup_time_)
        latest_startup_time = tmp_ds->startup_time_;

      add_tp(&machine_ltp_total, &old_ds_copy, 1);
      time_interval_total += time_interval_tmp;

    }
    else
    {
      compute_tp(&(machine_ltp_total), time_interval_total / index_ip);
      time_interval_total = 0;
      add_tp(&(cluster_ltp), &(machine_ltp_total), 1);
      if (flag == 1)
      {
        fprintf(stdout,"%-15s %-2d %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %6s %5"PRI64_PREFIX"d %6s %"PRI64_PREFIX"d %3s %"PRI64_PREFIX"d %5s %"PRI64_PREFIX"d %3s %"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n",
                tmp_ip,
                index_ip,
                Func::format_size(use_machine_cap).c_str(),
                Func::format_size(total_machine_cap).c_str(),
                static_cast<int32_t> (use_machine_cap * 100 / total_machine_cap),
                machine_blk_count,
                machine_load,
                Func::format_size(machine_tp.write_byte_).c_str(),
                machine_tp.write_file_count_,
                Func::format_size(machine_tp.read_byte_).c_str(),
                machine_tp.read_file_count_,
                Func::format_size(machine_ltp_total.write_byte_).c_str(),
                machine_ltp_total.write_file_count_,
                Func::format_size(machine_ltp_total.read_byte_).c_str(),
                machine_ltp_total.read_file_count_,
                Func::format_size(max_write_ltp.write_byte_).c_str(),
                max_write_ltp.write_file_count_,
                Func::format_size(max_read_ltp.read_byte_).c_str(),
                max_read_ltp.read_file_count_
          );
      }
      else
      {
        fprintf(stdout, "%-15s %-2d %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %3s %"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %-19s\n",
                tmp_ip,
                index_ip,
                Func::format_size(use_machine_cap).c_str(),
                Func::format_size(total_machine_cap).c_str(),
                static_cast<int32_t>(use_machine_cap * 100 / total_machine_cap),
                machine_blk_count,
                machine_load, Func::format_size(machine_ltp_total.write_byte_).c_str(),
                machine_ltp_total.write_file_count_,
                Func::format_size(machine_ltp_total.read_byte_).c_str(),
                machine_ltp_total.read_file_count_,
                Func::format_size(max_write_ltp.write_byte_).c_str(),
                max_write_ltp.write_file_count_,
                Func::format_size(max_read_ltp.read_byte_).c_str(),
                max_read_ltp.read_file_count_,
                Func::time_to_str(latest_startup_time).c_str());
      }
      ++total_machine;
      cluster_load += machine_load;
      index_ip = 1;
      use_machine_cap = tmp_ds->use_capacity_;
      total_machine_cap = tmp_ds->total_capacity_;
      machine_blk_count = tmp_ds->block_count_;
      memcpy(&machine_tp, &(tmp_ds->total_tp_), sizeof(struct Throughput));
      memcpy(&old_ds_copy, &(old_ds->total_tp_), sizeof(Throughput));
      memset(&machine_ltp_total, 0, sizeof(Throughput));
      memcpy(&max_read_ltp, &(old_ds->total_tp_), sizeof(Throughput));
      memcpy(&max_write_ltp, &(old_ds->total_tp_), sizeof(Throughput));
      memset(tmp_ip, '\0', 32);
      strncpy(tmp_ip, tmp_ip_next, strlen(tmp_ip_next));
      latest_startup_time = tmp_ds->startup_time_;
    }

    if (i == static_cast<int32_t>(ds_list.size()) - 1)
    {
      compute_tp(&(machine_ltp_total), (time_interval_total / index_ip));
      add_tp(&(cluster_ltp), &(machine_ltp_total), 1);
      if (flag == 1)
      {
        fprintf(stdout, "%-15s %-2d %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n",
                tmp_ip,
                index_ip,
                Func::format_size(use_machine_cap).c_str(),
                Func::format_size(total_machine_cap).c_str(),
                static_cast<int32_t>(use_machine_cap * 100 / total_machine_cap),
                machine_blk_count,
                machine_load,
                Func::format_size(machine_tp.write_byte_).c_str(),
                machine_tp.write_file_count_,
                Func::format_size(machine_tp.read_byte_).c_str(),
                machine_tp.read_file_count_,
                Func::format_size(machine_ltp_total.write_byte_).c_str(),
                machine_ltp_total.write_file_count_,
                Func::format_size(machine_ltp_total.read_byte_).c_str(),
                machine_ltp_total.read_file_count_,
                Func::format_size(max_write_ltp.write_byte_).c_str(),
                max_write_ltp.write_file_count_,
                Func::format_size(max_read_ltp.read_byte_).c_str(),
                max_read_ltp.read_file_count_);
      }
      else
      {

        fprintf(stdout, "%-15s %-2d %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %3s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %-19s\n",
                tmp_ip,
                index_ip,
                Func::format_size(use_machine_cap).c_str(),
                Func::format_size(total_machine_cap).c_str(),
                static_cast<int32_t>(use_machine_cap * 100 / total_machine_cap),
                machine_blk_count,
                machine_load,
                Func::format_size(machine_ltp_total.write_byte_).c_str(),
                machine_ltp_total.write_file_count_,
                Func::format_size(machine_ltp_total.read_byte_).c_str(),
                machine_ltp_total.read_file_count_,
                Func::format_size(max_write_ltp.write_byte_).c_str(),
                max_write_ltp.write_file_count_,
                Func::format_size(max_read_ltp.read_byte_).c_str(),
                max_read_ltp.read_file_count_,
                Func::time_to_str(latest_startup_time).c_str());
      }
      ++total_machine;
      cluster_load += machine_load;
    }
    memcpy(old_ds, ds, sizeof(DataServerStatInfo));

  }
  if (1 == flag)
  {
    fprintf(stdout, "Total : %-5d %-2Zd %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d\n",
            total_machine,
            ds_list.size(),
            Func::format_size(use_cluster_cap).c_str(),
            Func::format_size(total_cluster_cap).c_str(),
            static_cast<int32_t>(use_cluster_cap * 100 / total_cluster_cap),
            total_blk_count,
            (cluster_load / total_machine),
            Func::format_size(cluster_tp.write_byte_).c_str(),
            cluster_tp.write_file_count_,
            Func::format_size(cluster_tp.read_byte_).c_str(),
            cluster_tp.read_file_count_,
            Func::format_size(cluster_ltp.write_byte_).c_str(),
            cluster_ltp.write_file_count_,
            Func::format_size(cluster_ltp.read_byte_).c_str(),
            cluster_ltp.read_file_count_);
    printf(
      "------------- ---- ------------------ -------- ---- -----------  ----------  ----------  ---------  --------  ---------\n");
    printf(
      "SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD TOTAL_WRITE  TOTAL_READ  LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ\n");

  }
  else
  {
    fprintf(stdout, "Total : %-5d %-2Zd %6s %7s  %2d%%  %"PRI64_PREFIX"u   %u %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d\n",
            total_machine,
            ds_list.size(),
            Func::format_size(use_cluster_cap).c_str(),
            Func::format_size(total_cluster_cap).c_str(),
            static_cast<int32_t> (use_cluster_cap * 100 / total_cluster_cap),
            total_blk_count,
            (cluster_load / total_machine),
            Func::format_size(cluster_ltp.write_byte_).c_str(),
            cluster_ltp.write_file_count_,
            Func::format_size(cluster_ltp.read_byte_).c_str(),
            cluster_ltp.read_file_count_);
    printf("------------- ---- ------------------ -------- ---- ----------  ---------  --------  ---------\n");
    printf("SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ\n");
  }

  delete tmp_ds;
  tmp_ds = NULL;
}

void print_server_map_ex(SERVER_MAP* ds_map)
{
  int32_t count = 0;
  printf("SERVER_ADDR           CNT WBLOCK (PWBLOCK)\n");
  printf(
      "--------------------- --- ----------------------------------------------------------------------------------------\n");
  for (SERVER_MAP_ITER it = ds_map->begin(); it != ds_map->end(); it++)
  {
    ServerCollect* ser_vcol = it->second;
    DataServerStatInfo* ds = ser_vcol->get_ds();
    set <uint32_t>* m = const_cast< set <uint32_t>* > (ser_vcol->get_writable_block_list());
    set <uint32_t>* v = const_cast< set <uint32_t>* > (ser_vcol->get_primary_writable_block_list());
    if (report && static_cast<int32_t>(v->size()) >= max_write_filecount)
    {
      continue;
    }

    printf("%-21s %-3Zd ", get_addr_string(ds->id_), m->size());
    for (set<uint32_t>::iterator itx = m->begin(); itx != m->end(); itx++)
    {
      printf("%u ",*itx);
    }
    printf(" (");
    for (set<uint32_t>::iterator itv = v->begin(); itv != v->end(); itv++)
    {
      printf("%u ", *itv);
    }
    printf(")\n");
    count++;
  }
  if (0 == count)
  {
    printf("ALL OK\n\n");
  }
}

void print_wblock_list(VUINT* wblock_list)
{
  printf("WBLIST: (%Zd) => ", wblock_list->size());
  for (uint32_t i = 0; i < wblock_list->size(); i++)
  {
    printf("%u ", wblock_list->at(i));
  }
  printf("\n\n");
}

int show_block_list(Client* client, int32_t type)
{
  GetBlockListMessage gblm;
  gblm.request_flag_ = 1;
  gblm.write_flag_ = (type & 0x2) ? 1 : 0;
  gblm.start_block_id_ = 0;
  gblm.start_inclusive_ = 1;
  gblm.end_block_id_ = 0;
  gblm.read_count_ = 1000;
  gblm.return_count_ = 0;
  gblm.start_block_chunk_ = 0;
  gblm.next_block_chunk_ = 0;

  while (true)
  {
    Message* msg = client->call(&gblm);
    if (!msg)
    {
      printf("call GetBlockListMessage failed\n");
      break;
    }
    else
    {
      if (msg->get_message_type() == GET_BLOCK_LIST_MESSAGE)
      {
        GetBlockListMessage* resp_gbl_msg = dynamic_cast<GetBlockListMessage*>(msg);
        if (!(type & 0x1))
        {
          print_block_map(resp_gbl_msg->get_block_map());
        }
        else if (type & 0x1)
        {
          print_block_map_ex(resp_gbl_msg->get_block_map());
        }
        if (resp_gbl_msg->next_block_chunk_ < 0 || resp_gbl_msg->read_count_ != resp_gbl_msg->return_count_)
        {
          delete msg;
          break;
        }
        gblm.start_block_chunk_ = resp_gbl_msg->next_block_chunk_;
        gblm.start_block_id_ = resp_gbl_msg->end_block_id_;
        gblm.start_inclusive_ = 0;
        delete msg;
      }
      else
      {
        printf("return message type not matched\n");
        delete msg;
        break;
      }
    }
  }

  return 0;
}

int show_server_status(Client* client, int32_t type)
{
  GetServerStatusMessage req_gss_msg;
  int32_t ret_status = TFS_ERROR;
  int32_t xtype = type;
  if (type & 8)
  {
    xtype |= SSM_BLOCK;
  }
  if (type & 16)
  {
    xtype |= SSM_SERVER;
  }
  if (type & 32)
   {
    xtype |= SSM_SERVER;
   }
  if (type & 64)
  {
    xtype |= SSM_SERVER;
  }

  int32_t from_row = 0;
  int32_t return_row = 10000;
  req_gss_msg.set_status_type(xtype);
  req_gss_msg.set_from_row(from_row);
  req_gss_msg.set_return_row(return_row);

  BLOCK_MAP* block_map = NULL;
  SERVER_MAP* ds_map = NULL;
  VUINT* wblock_list = NULL;
  int32_t has_next = 0;

  Message* message = client->call(&req_gss_msg);
  if (NULL == message)
  {
    return (ret_status);
  }
  if (message->get_message_type() == SET_SERVER_STATUS_MESSAGE)
  {
    SetServerStatusMessage* req_sss_msg = dynamic_cast<SetServerStatusMessage*>(message);
    block_map = const_cast<BLOCK_MAP*> (req_sss_msg->get_block_map());
    ds_map = const_cast<SERVER_MAP*> (req_sss_msg->get_server_map());
    wblock_list = reinterpret_cast<VUINT*>(const_cast<VUINT32*> (req_sss_msg->get_wblock_list()));
    has_next = req_sss_msg->has_next();
    ret_status = TFS_SUCCESS;
  }
  else if (message->get_message_type() == STATUS_MESSAGE)
  {
    StatusMessage* req_sss_msg = dynamic_cast<StatusMessage*>(message);
    printf("%s\n", req_sss_msg->get_error());
    delete message;
    return (ret_status);
  }
  if ((xtype & SSM_BLOCK))
  {
    req_gss_msg.set_status_type(SSM_BLOCK);
    while (has_next)
    {
      has_next = 0;
      from_row += return_row;
      req_gss_msg.set_from_row(from_row);
      Message* ret_msg = client->call(&req_gss_msg);
      if (ret_msg == NULL)
      {
        break;
      }
      if (ret_msg->get_message_type() == SET_SERVER_STATUS_MESSAGE)
      {
        SetServerStatusMessage* xsm = dynamic_cast<SetServerStatusMessage*>(ret_msg);
        xsm->set_need_free(0);
        BLOCK_MAP* tmp_map = const_cast<BLOCK_MAP *>(xsm->get_block_map());
        BLOCK_MAP_ITER xit = tmp_map->begin();
        for (; xit != tmp_map->end(); xit++)
        {
          block_map->insert(BLOCK_MAP::value_type(xit->first, xit->second));
        }
        has_next = xsm->has_next();
      }
      delete ret_msg;
    }
  }

  if (type & 1)
    print_block_map(block_map);
  if (type & 2)
    print_server_map(ds_map);
  if (type & 4)
    print_wblock_list(wblock_list);
  if (type & 8)
    print_block_map_ex(block_map);
  if (type & 16)
    print_server_map_ex(ds_map);
  if (type & 32)
    print_machine(ds_map, 1);
  if (type & 64)
    print_machine(ds_map, 2);

  delete message;
  return ret_status;
}

static void usage(const char* name)
{
	fprintf(stdout, "Usage: %s -f tfs.conf -t type -i interval -c execute_count\n", name);
}

int main(int argc, char *argv[])
{
  int32_t i;
  int32_t type = 0xFFFF;
  int32_t interval = 1;
  int32_t exec_count = 1;
  int32_t only_block = -1;
  std::string file_name;
  while ((i = getopt(argc, argv, "f:t:i:c:h:rb:")) != EOF)
  {
    switch (i)
    {
    case 'f':
      file_name = optarg;
      break;
    case 't':
      type = atoi(optarg);
      break;
    case 'i':
      interval = atoi(optarg);
      break;
    case 'c':
      exec_count = atoi(optarg);
      break;
    case 'r':
      report = 1;
      break;
    case 'b':
      only_block = atoi(optarg);
      break;
    case 'h':
		default:
			usage(argv[0]);
      return TFS_ERROR;
    }
  }
	if (file_name.empty()
			|| file_name.compare(" ") == 0)
	{
		usage(argv[0]);
		return TFS_ERROR;
	}
  if (CONFIG.load(file_name) == TFS_ERROR)
  {
		TBSYS_LOG(ERROR, "load conf(%s) failed", file_name.c_str());
    return TFS_ERROR;
  }

  min_replication = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_MIN_REPLICATION, 3);
  max_block_size = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_BLOCK_MAX_SIZE);
  max_write_filecount = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_MAX_WRITE_FILECOUNT);
  const char* ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
  int32_t port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
  uint64_t master_ip = Func::str_to_addr(ip, port);
  int32_t ret = 0;
  const char* group_mask_str = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_GROUP_MASK);
  if (group_mask_str != NULL)
  {
    group_mask = Func::get_addr(group_mask_str);
  }
  if ((type & 2) || (type & 16))
  {
    load_last_ds();
  }

  Client* client = CLIENT_POOL.get_client(master_ip);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }

  signal(SIGINT, sign_handler);

  if (only_block >= 0)
  {
    show_block_list(client, only_block);
    goto out;
  }

  for (i = 0; exec_count == 0 || i < exec_count; i++)
  {
    ret = show_server_status(client, type);
    if (exec_count == i + 1)
      break;
    if (stop)
      break;
    Func::sleep(interval, &stop);
  }
  out:
	client->disconnect();
  if ((type & 2) || (type & 16))
  {
    save_last_ds();
  }

  for (DATASERVER_MAP_ITER it = ds_map_.begin(); it != ds_map_.end(); it++)
  {
		tbsys::gDelete(it->second);
  }
  CLIENT_POOL.release_client(client);

  fprintf(stdout,"%s Done.\n", (ret ? "FAILURE" : "SUCCESS"));
  return ret;
}


