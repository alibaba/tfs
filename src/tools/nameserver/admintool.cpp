#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "tbsys.h"

#include "common/internal.h"
#include "common/client_manager.h"
#include "common/config_item.h"
#include "common/status_message.h"
#include "common/new_client.h"
#include "message/server_status_message.h"
#include "message/client_cmd_message.h"
#include "message/message_factory.h"
#include "new_client/fsname.h"
#include "new_client/tfs_client_api.h"
#include "tools/util/tool_util.h"
#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::tools;
using namespace std;

#if defined(__DATE__) && defined(__TIME__) && defined(PACKAGE) && defined(VERSION)
static const char g_build_description[] = "Taobao file system(TFS), Version: " VERSION ", Build time: "__DATE__ " "__TIME__;
#else
static const char _g_build_description[] = "unknown";
#endif

static TfsClient* g_tfs_client = NULL;
static STR_FUNC_MAP g_cmd_map;
static const int32_t CMD_MAX_LEN = 4096;

int usage(const char *name);
static void sign_handler(const int32_t sig);
int main_loop();
int do_cmd(char* buf);
void init();
void version();
int get_file_retry(char* tfs_name, char* local_file);

/* cmd func */
int cmd_show_help(const VSTRING&);
int cmd_quit(const VSTRING&);
int cmd_set_run_param(const VSTRING& param);
int cmd_add_block(const VSTRING& param);
int cmd_remove_block(const VSTRING& param);
int cmd_list_block(const VSTRING& param);
int cmd_load_block(const VSTRING& param);
int cmd_compact_block(const VSTRING& param);
int cmd_replicate_block(const VSTRING& param);
int cmd_repair_group_block(const VSTRING& param);
int cmd_access_stat_info(const VSTRING& param);
int cmd_access_control_flag(const VSTRING& param);
int cmd_rotate_log(const VSTRING& param);
int cmd_dump_plan(const VSTRING &param);
int cmd_set_bpr(const VSTRING &param);
int cmd_get_bpr(const VSTRING &param);
int cmd_batch_file(const VSTRING& param);
int cmd_clear_system_table(const VSTRING& param);

template<class T> const char* get_str(T it)
{
  return it->first.c_str();
}

template<> const char* get_str(VSTRING::iterator it)
{
  return (*it).c_str();
}

template<class T> char* do_match(const char* text, int state, T& m)
{
  static typename T::iterator it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = m.begin();
    len = strlen(text);
  }

  while(it != m.end())
  {
    cmd = get_str(it);
    it++;
    if (strncmp(cmd, text, len) == 0)
    {
      int32_t cmd_len = strlen(cmd) + 1;
      // memory will be freed by readline
      return strncpy(new char[cmd_len], cmd, cmd_len);
    }
  }
  return NULL;
}

char* match_cmd(const char* text, int32_t state)
{
  return do_match(text, state, g_cmd_map);
}

char** admin_cmd_completion (const char* text, int, int)
{
  // disable default filename completion
  rl_attempted_completion_over = 1;
  return rl_completion_matches(text, match_cmd);
}
#endif

void init()
{
  g_cmd_map["help"] = CmdNode("help", "show help info", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit", "quit", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit", "exit", 0, 0, cmd_quit);
  g_cmd_map["param"] = CmdNode("param name [set value [extravalue]]", "set/get param value", 0, 4, cmd_set_run_param);
  g_cmd_map["addblk"] = CmdNode("addblk blockid", "add block", 1, 1, cmd_add_block);
  g_cmd_map["removeblk"] = CmdNode("removeblk blockid [serverip:port|flag]",
      "remove block. flag: 1--remove block from both ds and ns, 2--just relieve relation from ns, default is 1.",
      1, 2, cmd_remove_block);
  g_cmd_map["listblk"] = CmdNode("listblk blockid", "list block server list", 1, 1, cmd_list_block);
  g_cmd_map["loadblk"] = CmdNode("loadblk blockid dsip:port", "build relationship between block and dataserver.", 2, 2, cmd_load_block);
  g_cmd_map["clearsystemtable"] = CmdNode("clearsystemtable", "clear system table 1--task, 2--write block, 4--report block server, 8--delete block queue.", 1, 1, cmd_clear_system_table);
  g_cmd_map["compactblk"] = CmdNode("compactblk blockid", "compact block", 1, 1, cmd_compact_block);
  g_cmd_map["replblk"] = CmdNode("replblk blockid type [[action] [src] [dest]]",
      "replicate block. type: 1--action, 2--src, 3--dest, 4--action src, 5--action dest, 6--src dest, 7--action src dest",
      2, 5, cmd_replicate_block);
  g_cmd_map["aci"] = CmdNode("aci dsip:port [startrow returnrow]", "get dataserver access information, such as write or read times", 1, 3, cmd_access_stat_info);
  g_cmd_map["setacl"] = CmdNode("setacl dsip:port type [v1 [v2]]",
      "set dataserver access control. it can reject the request of certain ip or network segment"
      "type: 1--ACL_FLAG, 2--ACL_IPMASK, 3--ACL_IPLIST, 4--ACL_CLEAR, 5--ACL_RELOAD",
      2, 4, cmd_access_control_flag);
  g_cmd_map["rotatelog"] = CmdNode("rotatelog","rotate log file. it will move nameserver.log to nameserver.log.currenttime, and create new nameserver.log",
      0, 0, cmd_rotate_log);
  g_cmd_map["dumpplan"] = CmdNode("dumpplan [nsip:port]", "dump plan server", 0, 1, cmd_dump_plan);
  g_cmd_map["setbpr"] = CmdNode("setbpr value1 value2",
      "set balance percent ratio. value1: integer part, 0 or 1, value2 should be 0 if value1 is 1. value2: float part.",
      2, 2, cmd_set_bpr);
  g_cmd_map["getbpr"] = CmdNode("getbpr", "get balance percent ratio, float value, ex: 1.000000 or 0.000005", 0, 0, cmd_get_bpr);
  g_cmd_map["batch"] = CmdNode("batch file", "batch run command in file", 1, 1, cmd_batch_file);
}

void version()
{
  std::cerr << g_build_description << std::endl;
  exit(TFS_SUCCESS);
}

// expand ~ to HOME. modify argument
const char* expand_path(string& path)
{
  if (path.size() > 0 && '~' == path.at(0) &&
      (1 == path.size() ||                      // just one ~
       (path.size() > 1 && '/' == path.at(1)))) // like ~/xxx
  {
    char* home_path = getenv("HOME");
    if (NULL == home_path)
    {
      fprintf(stderr, "can't get HOME path: %s\n", strerror(errno));
    }
    else
    {
      path.replace(0, 1, home_path);
    }
  }
  return path.c_str();
}

int cmd_batch_file(const VSTRING& param)
{
  const char* batch_file = expand_path(const_cast<string&>(param[0]));
  FILE* fp = fopen(batch_file, "rb");
  int ret = TFS_SUCCESS;
  if (fp == NULL)
  {
    fprintf(stderr, "open file error: %s\n\n", batch_file);
    ret = TFS_ERROR;
  }
  else
  {
    int32_t error_count = 0;
    int32_t count = 0;
    VSTRING params;
    char buffer[MAX_CMD_SIZE];
    while (fgets(buffer, MAX_CMD_SIZE, fp))
    {
      if ((ret = do_cmd(buffer)) == TFS_ERROR)
      {
        error_count++;
      }
      if (++count % 100 == 0)
      {
        fprintf(stdout, "tatol: %d, %d errors.\r", count, error_count);
        fflush(stdout);
      }
      if (TFS_CLIENT_QUIT == ret)
      {
        break;
      }
    }
    fprintf(stdout, "tatol: %d, %d errors.\n\n", count, error_count);
    fclose(fp);
  }
  return TFS_SUCCESS;
}

int cmd_get_bpr(const VSTRING& param)
{
  UNUSED(param);
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_GET_BALANCE_PERCENT);

  tbnet::Packet* ret_message = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int iret = send_msg_to_server(g_tfs_client->get_server_id(), client, &req_cc_msg, ret_message);
  string ret_value = "successful ";
  if (TFS_SUCCESS != iret)
  {
    ret_value = "getbpr failed ";
  }
  else
  {
    if (ret_message->getPCode() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
      iret = s_msg->get_status();
      ret_value += s_msg->get_error();
    }
  }
  ToolUtil::print_info(iret, "%s", ret_value.c_str());

  NewClientManager::get_instance().destroy_client(client);
  return iret;
}

int cmd_set_bpr(const VSTRING& param)
{
  int32_t size = param.size();
  int32_t iret = 2 != size ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS != iret)
  {
    fprintf(stderr, "parameter size: %d is invalid, must be 2\n", size);
  }
  else
  {
    int32_t value3 = atoi(param[0].c_str());
    int32_t value4;
    iret = (value3 > 1 || value3 < 0 || param[1].length() > 6 || ((value4 = atoi(param[1].c_str())) != 0 && 1 == value3)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
    if (TFS_SUCCESS != iret)
    {
      fprintf(stderr, "parameter is invalid, value1: (0|1), value2.length < 6. value1: %d, value2: %s\n", value3, param[1].c_str());
    }
    else
    {
      ClientCmdMessage req_cc_msg;
      req_cc_msg.set_cmd(CLIENT_CMD_SET_BALANCE_PERCENT);
      req_cc_msg.set_value3(value3);
      req_cc_msg.set_value4(value4);

      tbnet::Packet* ret_message = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      iret = send_msg_to_server(g_tfs_client->get_server_id(), client, &req_cc_msg, ret_message);
      string ret_value = "successful";
      if (TFS_SUCCESS != iret)
      {
        ret_value = "setbpr failed";
      }
      else
      {
        if (ret_message->getPCode() == STATUS_MESSAGE)
        {
          StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
          iret = s_msg->get_status();
          if (iret != STATUS_MESSAGE_OK)
          {
            ret_value = s_msg->get_error();
          }
        }
      }
      ToolUtil::print_info(iret, "%s", ret_value.c_str());

      NewClientManager::get_instance().destroy_client(client);
    }
  }
  return iret;
}

int cmd_set_run_param(const VSTRING& param)
{
  /*const static char* param_str[] = {
      "min_replication",
      "max_replication",
      "max_write_file_count",
      "max_use_capacity_ratio",
      "heart_interval",
      "replicate_wait_time",
      "compact_delete_ratio",
      "compact_max_load",
      "plan_run_flag",
      "run_plan_expire_interval",
      "run_plan_ratio",
      "object_dead_max_time",
      "balance_max_diff_block_num",
      "log_level",
      "add_primary_block_count",
      "build_plan_interval",
      "replicate_ratio",
      "max_wait_write_lease",
      "dispatch_oplog",
      "cluster_index",
      "build_plan_default_wait_time",
      "group_count",
      "group_seq",
      "discard_newblk_safe_mode_time",
      "discard_max_count",
      "strategy_write_capacity_weigth",
      "strategy_write_elect_num_weigth",
      "strategy_replicate_capactiy_weigth",
      "strategy_replicate_load_weigth",
      "strategy_replicate_elect_num_weigth"
      ""
  };*/
  static int32_t param_strlen = sizeof(dynamic_parameter_str) / sizeof(char*);

  int32_t size = param.size();
  if (size != 1 && size != 3)
  {
    fprintf(stderr, "param param_name\n\n");
    for (int32_t i = 0; i < param_strlen; i++)
    {
      fprintf(stderr, "%s\n", dynamic_parameter_str[i]);
    }
    return TFS_ERROR;
  }

  const char* param_name = param[0].c_str();
  uint32_t index = 0;
  for (int32_t i = 0; i < param_strlen; i++)
  {
    if (strcmp(param_name, dynamic_parameter_str[i]) == 0)
    {
      index = i + 1;
      break;
    }
  }

  if (0 == index)
  {
    fprintf(stderr, "param %s not valid\n", param_name);
    return TFS_ERROR;
  }

  int32_t value = 0;
  bool is_set = false;

  if (3 == size)
  {
    if (strcmp("set", param[1].c_str()))
    {
      fprintf(stderr, "param %s set value\n\n", param_name);
      return TFS_ERROR;
    }
    index |= 0x10000000;
    is_set = true;
    value = atoi(param[2].c_str());
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_value3(index);
  req_cc_msg.set_value4(value);

  tbnet::Packet* ret_message = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  send_msg_to_server(g_tfs_client->get_server_id(), client, &req_cc_msg, ret_message);

  int ret = TFS_SUCCESS;
  string ret_value = "";
  if (ret_message == NULL)
  {
    ret = TFS_ERROR;
  }
  else if (ret_message->getPCode() == STATUS_MESSAGE)
  {
    StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
    ret = s_msg->get_status();
    if (ret == STATUS_MESSAGE_OK && !is_set)
    {
      ret_value = s_msg->get_error();
    }
  }

  string ret_str = "param " +
    (is_set ? (string(param_name) + " set " + param[2]) :
     string(param_name) + " " + ret_value);

  ToolUtil::print_info(ret, "%s", ret_str.c_str());

  NewClientManager::get_instance().destroy_client(client);

  return ret;
}

int cmd_add_block(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());

  if (0 == block_id)
  {
    fprintf(stderr, "block_id: %u\n\n", block_id);
    return TFS_ERROR;
  }

  VUINT64 ds_list;

  int ret = ToolUtil::get_block_ds_list(g_tfs_client->get_server_id(), block_id, ds_list, T_WRITE|T_NEWBLK|T_NOLEASE);

  ToolUtil::print_info(ret, "add block %u", block_id);

  return ret;
}

int cmd_remove_block(const VSTRING& param)
{
  uint32_t flag = 0;
  uint32_t block_id = atoi(param[0].c_str());
  uint64_t server_id = 0;
  if (param.empty())
  {
    fprintf(stderr, "invalid parameter, param.empty\n");
    return TFS_ERROR;
  }
  if (param.size() == 1)
  {
    flag = 1;
  }
  else if (param.size() == 2 )
  {
    if (param[1].length() == 1)
      flag = atoi(param[1].c_str());
    else
    {
      server_id = Func::get_host_ip(param[1].c_str());
      if (0 == server_id)
      {
        fprintf(stderr, "invalid addr %s\n", param[1].c_str());
        return TFS_ERROR;
      }
    }
  }
  if (0 == block_id)
  {
    fprintf(stderr, "invalid blockid %s\n", param[0].c_str());
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_value1(server_id);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(flag);

  int32_t status = TFS_ERROR;

  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);

  if (param.size() == 1)
    ToolUtil::print_info(status, "removeblock %s", param[0].c_str());
  else if (param.size() == 2)
    ToolUtil::print_info(status, "removeblock %s %s", param[0].c_str(), param[1].c_str());
  return status;
}

int cmd_list_block(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());
  int ret = TFS_ERROR;

  if (block_id <= 0)
  {
    fprintf(stderr, "invalid block id: %u\n", block_id);
  }
  else
  {
    VUINT64 ds_list;
    ret = ToolUtil::get_block_ds_list(g_tfs_client->get_server_id(), block_id, ds_list);
    ToolUtil::print_info(ret, "list block %u", block_id);

    if (TFS_SUCCESS == ret)
    {
      int32_t ds_size = ds_list.size();
      fprintf(stdout, "------block: %u, has %d replicas------\n", block_id, ds_size);
      for (int32_t i = 0; i < ds_size; ++i)
      {
        fprintf(stdout, "block: %u, (%d)th server: %s \n", block_id, i, tbsys::CNetUtil::addrToString(ds_list[i]).c_str());
      }
    }
  }
  return ret;
}

int cmd_load_block(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());
  uint64_t server_id = Func::get_host_ip(param[1].c_str());
  if (0 == server_id || 0 == block_id)
  {
    fprintf(stderr, "invalid blockid or address: %s %s\n", param[0].c_str(), param[1].c_str());
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_LOADBLK);
  req_cc_msg.set_value1(server_id);
  req_cc_msg.set_value3(block_id);

  int status = TFS_ERROR;

  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);

  ToolUtil::print_info(status, "loadblock %s %s", param[0].c_str(), param[1].c_str());

  return status;
}

int cmd_compact_block(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());
  if (0 == block_id)
  {
    fprintf(stderr, "invalid block id: %u\n", block_id);
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_COMPACT);
  req_cc_msg.set_value3(block_id);

  int32_t status = TFS_ERROR;
  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);
  ToolUtil::print_info(status, "compactblock %u", block_id);

  return status;
}

int cmd_replicate_block(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());
  int32_t op_type = atoi(param[1].c_str());
  if (0 == block_id || op_type > 7 || op_type < 1)
  {
    fprintf(stderr, "invalid param. blockid > 0, type in [1, 7]. blockid: %u, op_type: %d\n", block_id, op_type);
    return TFS_ERROR;
  }

  ReplicateBlockMoveFlag flag = REPLICATE_BLOCK_MOVE_FLAG_NO;
  uint64_t src_id = 0;
  uint64_t dest_id = 0;
  string action = "no";

  int32_t size = param.size();
  switch (op_type)
  {
  case 1:
    if (size != 3)
    {
      fprintf(stderr, "replblk blockid 1 action\n");
      fprintf(stderr, "action: yes -- move block\n");
      fprintf(stderr, "        no -- replicate block\n");
      return TFS_ERROR;
    }
    action = param[2];
    break;
  case 2:
    if (size != 3)
    {
      fprintf(stderr, "replblk blockid 2 src\n");
      return TFS_ERROR;
    }
    src_id = Func::get_host_ip(param[2].c_str());
    if (src_id == 0)
    {
      fprintf(stderr, "src addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  case 3:
    if (size != 3)
    {
      fprintf(stderr, "replblk blockid 3 dest\n");
      return TFS_ERROR;
    }
    dest_id = Func::get_host_ip(param[2].c_str());
    if (dest_id == 0)
    {
      fprintf(stderr, "src addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  case 4:
    if (size != 4)
    {
      fprintf(stderr, "replblk blockid 4 action src\n");
      fprintf(stderr, "action: yes -- move block\n");
      fprintf(stderr, "        no -- replicate block\n");
      return TFS_ERROR;
    }
    action = param[2];
    src_id = Func::get_host_ip(param[3].c_str());
    if (src_id == 0)
    {
      fprintf(stderr, "src addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  case 5:
    if (size != 4)
    {
      fprintf(stderr, "replblk blockid 5 action dest\n");
      fprintf(stderr, "action: yes -- move block\n");
      fprintf(stderr, "        no -- replicate block\n");
      return TFS_ERROR;
    }
    action = param[2];
    dest_id = Func::get_host_ip(param[3].c_str());
    if (dest_id == 0)
    {
      fprintf(stderr, "dest addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  case 6:
    if (size != 4)
    {
      fprintf(stderr, "replblk blockid 6 src dest\n");
      return TFS_ERROR;
    }
    src_id = Func::get_host_ip(param[2].c_str());
    dest_id = Func::get_host_ip(param[3].c_str());
    if (src_id == 0 || dest_id == 0)
    {
      fprintf(stderr, "src or dest addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  case 7:
    if (size != 5)
    {
      fprintf(stderr, "replblk blockid 7 action src dest\n");
      return TFS_ERROR;
    }
    action = param[2];
    src_id = Func::get_host_ip(param[3].c_str());
    dest_id = Func::get_host_ip(param[4].c_str());
    if (src_id == 0 || dest_id == 0)
    {
      fprintf(stderr, "src or dest addr is not correct, please check it\n");
      return TFS_ERROR;
    }
    break;
  default:
    fprintf(stderr, "error type %d must in [1,7]\n\n", op_type);
    return TFS_ERROR;
  }

  if (action == "yes")
  {
    flag = REPLICATE_BLOCK_MOVE_FLAG_YES;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_IMMEDIATELY_REPL);
  req_cc_msg.set_value1(src_id);
  req_cc_msg.set_value2(dest_id);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(flag);

  int32_t status = TFS_ERROR;
  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);
  ToolUtil::print_info(status, "replicate block %u %"PRI64_PREFIX"u %"PRI64_PREFIX"u", block_id, src_id, dest_id);

  return status;
}

int cmd_repair_group_block(const VSTRING&)
{
  // uint32_t block_id = atoi(param[0].c_str());

  // if (block_id <= 0)
  // {
  //   fprintf(stderr, "invalid block id %u\n", block_id);
  //   return TFS_ERROR;
  // }

  //   ClientCmdMessage req_cc_msg;
  //   req_cc_msg.set_cmd(CLIENT_CMD_REPAIR_GROUP);
  //   req_cc_msg.set_value3(block_id);
  //   req_cc_msg.set_value4(0);
  //   req_cc_msg.set_value1(g_local_server_ip);

  //   int32_t status = TFS_ERROR;
  //   send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);
  //   ToolUtil::print_info(status, "repairgrp %u", block_id);

  //   return status;
  return TFS_SUCCESS;
}

int cmd_access_stat_info(const VSTRING& param)
{
  int32_t size = param.size();
  uint64_t server_id = Func::get_host_ip(param[0].c_str());

  uint32_t start_row = 0;
  uint32_t return_row = 0;

  if (size > 1)
  {
    start_row = atoi(param[1].c_str());
  }
  if (size > 2)
  {
    return_row = atoi(param[2].c_str());
  }

  bool get_all = (start_row == 0 && return_row == 0);
  if (get_all)
  {
    return_row = 1000;
  }
  int32_t has_next = 0;

  GetServerStatusMessage req_gss_msg;

  fprintf(stdout,
          "ip addr           | read count  | read bytes  | write count  | write bytes\n"
          "------------------ -------------- ------------- -------------- ------------\n");

  int ret = TFS_SUCCESS;
  while (1)
  {
    req_gss_msg.set_status_type(GSS_CLIENT_ACCESS_INFO);
    req_gss_msg.set_from_row(start_row);
    req_gss_msg.set_return_row(return_row);

    tbnet::Packet* ret_message = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    send_msg_to_server(server_id, client, &req_gss_msg, ret_message);

    if (ret_message == NULL)
    {
      ret = TFS_ERROR;
    }
    else if (ret_message->getPCode() == ACCESS_STAT_INFO_MESSAGE)
    {
      AccessStatInfoMessage* req_cb_msg = reinterpret_cast<AccessStatInfoMessage*> (ret_message);
      const AccessStatInfoMessage::COUNTER_TYPE & m = req_cb_msg->get();
      for (AccessStatInfoMessage::COUNTER_TYPE::const_iterator it = m.begin(); it != m.end(); ++it)
      {
        printf("%15s : %14" PRI64_PREFIX "u %14s %14" PRI64_PREFIX "u %14s\n",
               tbsys::CNetUtil::addrToString(it->first).c_str(),
               it->second.read_file_count_, Func::format_size(it->second.read_byte_).c_str(),
               it->second.write_file_count_, Func::format_size(it->second.write_byte_).c_str());
      }

      has_next = req_cb_msg->has_next();
    }
    else if (ret_message->getPCode() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
      fprintf(stderr, "get status msg, ret: %d, error: %s\n", s_msg->get_status(), s_msg->get_error());
      ret = s_msg->get_status();
    }

    NewClientManager::get_instance().destroy_client(client);

    if (TFS_SUCCESS == ret)
    {
      if (get_all)
      {
        if (!has_next)
        {
          break;
        }
        else
        {
          start_row += return_row;
        }
      }
      else
      {
        break;
      }
    }
    else
    {
      break;
    }
  }
  return ret;
}

int cmd_access_control_flag(const VSTRING& param)
{
  int32_t size = param.size();
  uint64_t server_id = Func::get_host_ip(param[0].c_str());
  uint32_t op_type = atoi(param[1].c_str());
  if (op_type < 1 || op_type > 5)
  {
    fprintf(stderr, "error type %d must in [1,5]\n\n", op_type);
    return TFS_ERROR;
  }

  const char* value1 = NULL;
  const char* value2 = NULL;
  uint64_t v1 = 0;
  uint32_t v2 = 0;
  if (size > 2)
  {
    value1 = param[2].c_str();
  }

  if (size > 3)
  {
    value2 = param[3].c_str();
  }

  switch (op_type)
  {
  case 1:
    if (!value1)
    {
      fprintf(stderr, "setacl ip:port 1 flag\n");
      fprintf(stderr, "flag: 0 -- set mode, when you can set ip mask or ip port\n");
      fprintf(stderr, "      1 -- control mode, when those in ip list will be denied to do read or readV2 operation\n");
      fprintf(stderr, "      2 -- control mode, when those in ip list will be denied to do write or close operation\n");
      fprintf(stderr, "      4 -- control mode, when those in ip list will be denied to do unlink operation\n");
      return TFS_ERROR;
    }
    v1 = atoi(value1);
    v2 = 0;
    break;
  case 2:
    if (!value1 || !value2)
    {
      fprintf(stderr, "setacl ip:port 2 ip mask\n");
      return TFS_ERROR;
    }
    v1 = tbsys::CNetUtil::strToAddr(const_cast<char*> (value1), 0);
    v2 = static_cast<uint32_t> (tbsys::CNetUtil::strToAddr(const_cast<char*> (value2), 0));
    if (!v1 || !v2)
    {
      fprintf(stderr, "setacl ip:port 2 ip mask, not  a valid ip & mask\n");
      return TFS_ERROR;
    }
    break;
  case 3:
    if (!value1)
    {
      fprintf(stderr, "setacl ip:port 3 ipaddr\n");
      return TFS_ERROR;
    }
    v1 = tbsys::CNetUtil::strToAddr(const_cast<char*> (value1), 0);
    v2 = 0;
    break;
  case 4:
  case 5:
    v1 = 0;
    v2 = 0;
    break;
  default:
    fprintf(stderr, "error type %d must in [1,5]\n\n", op_type);
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_value3(op_type); // param type == 1 as set acl flag.
  req_cc_msg.set_value1(v1); // ns_id as flag
  req_cc_msg.set_value4(v2);

  int32_t status = TFS_ERROR;
  send_msg_to_server(server_id, &req_cc_msg, status);
  ToolUtil::print_info(status, "set acl %s", param[0].c_str());
  return status;
}

int cmd_rotate_log(const VSTRING& param)
{
  UNUSED(param);
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_ROTATE_LOG);
  int32_t status = TFS_ERROR;

  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);
  ToolUtil::print_info(status, "rotate nameserver log, %s", tbsys::CNetUtil::addrToString(g_tfs_client->get_server_id()).c_str());
  return status;
}

int cmd_show_help(const VSTRING&)
{
  return ToolUtil::show_help(g_cmd_map);
}

int cmd_quit(const VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_get_file_retry(char*, char*)
{
//   fprintf(stderr, "filename: %s\n", tfs_name);
//   fflush( stderr);
//   int tfs_fd = 0;
//   tfs_fd = g_tfs_client->open(tfs_name, NULL, T_READ);
//   if (tfs_fd < 0)
//   {
//     fprintf(stderr, "open tfs_client fail\n");
//     return TFS_ERROR;
//   }
//   TfsFileStat file_info;
//   if (g_tfs_client->fstat(tfs_fd, &file_info) == TFS_ERROR)
//   {
//     g_tfs_client->close(tfs_fd);
//     fprintf(stderr, "fstat tfs_client fail\n");
//     return TFS_ERROR;
//   }

//   int32_t done = 0;
//   while (done >= 0 && done <= 10)
//   {
//     int64_t t1 = Func::curr_time();
//     int32_t fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
//     if (-1 == fd)
//     {
//       fprintf(stderr, "open local file fail: %s\n", local_file);
//       g_tfs_client->close(tfs_fd);
//       return TFS_ERROR;
//     }

//     char data[MAX_READ_SIZE];
//     uint32_t crc = 0;
//     int32_t total_size = 0;
//     for (;;)
//     {
//       int32_t read_len = g_tfs_client->read(tfs_fd, data, MAX_READ_SIZE);
//       if (read_len < 0)
//       {
//         fprintf(stderr, "read tfs_client fail\n");
//         break;
//       }
//       if (0 == read_len)
//       {
//         break;
//       }
//       if (write(fd, data, read_len) != read_len)
//       {
//         fprintf(stderr, "write local file fail: %s\n", local_file);
//         g_tfs_client->close(tfs_fd);
//         close(fd);
//         return TFS_ERROR;
//       }
//       crc = Func::crc(crc, data, read_len);
//       total_size += read_len;
//       if (read_len != MAX_READ_SIZE)
//       {
//         break;
//       }
//     }
//     close(fd);
//     if (crc == file_info.crc_ && total_size == file_info.size_)
//     {
//       g_tfs_client->close(tfs_fd);
//       return TFS_SUCCESS;
//     }

//     int64_t t2 = Func::curr_time();
//     if (t2 - t1 > 500000)
//     {
//       //fprintf(stderr, "filename: %s, time: %" PRI64_PREFIX "d, server: %s, done: %d\n", tfs_name, t2 - t1,
//       //   tbsys::CNetUtil::addrToString(tfs_client->get_last_elect_ds_id()).c_str(), done);
//       fflush(stderr);
//     }
//     done++;
//     /*if (tfs_client->tfs_reset_read() <= done)
//       {
//       break;
//       }*/
//   }
//   tfs_client->close(tfs_fd);
  return TFS_SUCCESS;
}

int main(int argc,char** argv)
{
  int32_t i;
  bool directly = false;
  bool set_log_level = false;
  const char* ns_ip = NULL;

  init();

  while ((i = getopt(argc, argv, "s:invh")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_ip = optarg;
        break;
      case 'i':
        directly = true;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'v':
        version();
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (NULL == ns_ip)
  {
    fprintf(stderr, "please input nameserver ip and port.\n");
    usage(argv[0]);
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  g_tfs_client = TfsClient::Instance();
  int ret = g_tfs_client->initialize(ns_ip, DEFAULT_BLOCK_CACHE_TIME, 1000, false);
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "init tfs client fail. ret: %d\n", ret);
    return ret;
  }


  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else
  {
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        do_cmd(argv[i]);
      }
    }
    else
    {
      usage(argv[0]);
    }
  }
}

int usage(const char *name)
{
  fprintf(stderr,
          "\n****************************************************************************** \n"
          "You can operate nameserver by this tool.\n"
          "Usage: \n"
          "  %s -s ns_ip_port [-i 'command'] [-v version] [-h help]\n"
          "****************************************************************************** \n\n",
          name);
  ToolUtil::show_help(g_cmd_map);
  exit(TFS_ERROR);
}

static void sign_handler(const int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "admintool exit.\n");
    exit(TFS_ERROR);
    break;
  default:
    break;
  }
}

inline bool is_whitespace(char c)
{
  return (' ' == c || '\t' == c);
}

inline char* strip_line(char* line)
{
  // trim start postion
  while(is_whitespace(*line))
  {
    line++;
  }

  // trim end postion
  int end_pos = strlen(line);
  while (end_pos && (is_whitespace(line[end_pos-1]) || '\n' == line[end_pos-1] || '\r' == line[end_pos-1]))
  {
    end_pos--;
  }
  line[end_pos] = '\0';

  // merge whitespace
  char new_line[CMD_MAX_LEN];
  snprintf(new_line, CMD_MAX_LEN + 1, "%s", line);

  int j = 0;
  for (int i = 0; i < end_pos; i++)
  {
    if (i+1 <= end_pos && line[i] == ' ' && line[i+1] == ' ')
    {
      continue;
    }
    new_line[j] = line[i];
    j++;
  }
  new_line[j] = '\0';
  snprintf(line, end_pos + 1, "%s", new_line);

  return line;
}

int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = admin_cmd_completion;
#else
  char cmd_line[CMD_MAX_LEN];
#endif
  int ret = TFS_ERROR;
  while (1)
  {
    std::string tips = "";
    tips = "TFS > ";
#ifdef _WITH_READ_LINE
    cmd_line = readline(tips.c_str());
    if (!cmd_line)
#else
      fprintf(stderr, tips.c_str());

    if (NULL == fgets(cmd_line, CMD_MAX_LEN, stdin))
#endif
    {
      break;
    }
    ret = do_cmd(cmd_line);
#ifdef _WITH_READ_LINE

    delete cmd_line;
    cmd_line = NULL;
#endif
    if (TFS_CLIENT_QUIT == ret)
    {
      break;
    }
  }
  return TFS_SUCCESS;
}

int32_t do_cmd(char* key)
{
  key = strip_line(key);
  if (!key[0])
  {
    return TFS_SUCCESS;
  }
#ifdef _WITH_READ_LINE
  // not blank line, add to history
  add_history(key);
#endif

  // check the validation of sub command
  char* token = strchr(key, ' ');
  if (token != NULL)
  {
    *token = '\0';
  }

  STR_FUNC_MAP_ITER it = g_cmd_map.find(Func::str_to_lower(key));

  if (it == g_cmd_map.end())
  {
    fprintf(stderr, "unknown command. \n");
    return TFS_ERROR;
  }

  if (token != NULL)
  {
    token++;
    key = token;
  }
  else
  {
    key = NULL;
  }

  // get param of sub command
  VSTRING param;
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if ('\0' == token[0])
    {
      break;
    }
    param.push_back(token);
  }
  // check param count
  int32_t param_cnt = param.size();
  if (param_cnt < it->second.min_param_cnt_ || param_cnt > it->second.max_param_cnt_)
  {
    fprintf(stderr, "%s\t\t%s\n\n", it->second.syntax_, it->second.info_);
    return TFS_ERROR;
  }

  return it->second.func_(param);
}

int cmd_dump_plan(const VSTRING& param)
{
  int32_t size = param.size();
  uint64_t server_id = g_tfs_client->get_server_id();

  if (size >= 1)
  {
    server_id = Func::get_host_ip(param[0].c_str());
  }

  DumpPlanMessage req_dp_msg;
  tbnet::Packet* ret_message = NULL;

  NewClient* client = NewClientManager::get_instance().create_client();

  int ret = send_msg_to_server(server_id, client, &req_dp_msg, ret_message);

  ToolUtil::print_info(ret, "%s", "dump plan");

  if (ret_message != NULL &&
      ret_message->getPCode() == DUMP_PLAN_RESPONSE_MESSAGE)
  {
    DumpPlanResponseMessage* req_dpr_msg = dynamic_cast<DumpPlanResponseMessage*>(ret_message);
    tbnet::DataBuffer& data_buff = req_dpr_msg->get_data();
    uint32_t plan_num = data_buff.readInt32();

    if (plan_num == 0)
    {
      printf("There is no plan currently.\n");
    }
    else
    {
      uint8_t plan_type;
      uint8_t plan_status;
      uint8_t plan_priority;
      uint32_t block_id;
      uint64_t plan_begin_time;
      uint64_t plan_end_time;
      int64_t plan_seqno;
      uint8_t server_num;
      std::string runer;
      uint8_t plan_complete_status_num;
      std::vector< std::pair <uint64_t, uint8_t> > plan_compact_status_vec;
      std::pair<uint64_t, uint8_t> plan_compact_status_pair;

      printf("Plan Number(running + pending):%d\n", plan_num);
      printf("seqno   type       status     priority   block_id   begin        end           runer  \n");
      printf("------  ---------  -------    ---------  --------   -----------  -----------  -------\n");

      for (uint32_t i=0; i<plan_num; i++)
      {
        plan_type = data_buff.readInt8();
        plan_status = data_buff.readInt8();
        plan_priority = data_buff.readInt8();
        block_id = data_buff.readInt32();
        plan_begin_time = data_buff.readInt64();
        plan_end_time = data_buff.readInt64();
        plan_seqno = data_buff.readInt64();
        server_num = data_buff.readInt8();
        runer = "";

        for (uint32_t j=0; j<server_num; j++)
        {
          runer += tbsys::CNetUtil::addrToString(data_buff.readInt64());
          runer += "/";
        }

        if (plan_type == PLAN_TYPE_COMPACT)
        {
          plan_complete_status_num = data_buff.readInt8();
          plan_complete_status_num = 0;
          plan_compact_status_vec.clear();
          for (uint32_t k=0; k<plan_complete_status_num; k++)
          {
            plan_compact_status_pair.first = data_buff.readInt64();
            plan_compact_status_pair.second = data_buff.readInt8();
            plan_compact_status_vec.push_back(plan_compact_status_pair);
          }
        }

        //display plan info
        printf("%-7"PRI64_PREFIX"d %-10s %-10s %-10s %-10u %-12"PRI64_PREFIX"d %-12"PRI64_PREFIX"d %-31s\n",
               plan_seqno,
               plan_type == PLAN_TYPE_REPLICATE ? "replicate" : plan_type == PLAN_TYPE_MOVE ? "move" : plan_type == PLAN_TYPE_COMPACT ? "compact" : plan_type == PLAN_TYPE_DELETE ? "delete" : "unknow",
               plan_status == PLAN_STATUS_BEGIN ? "begin" : plan_status == PLAN_STATUS_TIMEOUT ? "timeout" : plan_status == PLAN_STATUS_END ? "finish" : plan_status == PLAN_STATUS_FAILURE ? "failure": "unknow",
               plan_priority == PLAN_PRIORITY_NORMAL ? "normal" : plan_priority == PLAN_PRIORITY_EMERGENCY ? "emergency": "unknow",
               block_id, plan_begin_time, plan_end_time, runer.c_str());
      }
    }
  }
  else
  {
    fprintf(stderr, "invalid response message\n");
  }

  NewClientManager::get_instance().destroy_client(client);

  return ret;
}

int cmd_clear_system_table(const VSTRING& param)
{
  uint32_t value = atoi(param[0].c_str());
  if (0 == value)
  {
    fprintf(stderr, "invalid parameter, eg 1, 2, 4, 8: %s\n", param[0].c_str());
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_CLEAR_SYSTEM_TABLE);
  req_cc_msg.set_value3(value);

  int status = TFS_ERROR;

  send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, status);

  ToolUtil::print_info(status, "clear system table %s",param[0].c_str());

  return status;
}


