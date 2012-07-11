/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfstool.cpp 1000 2011-11-03 02:40:09Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "tbsys.h"

#include "common/internal.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/meta_server_define.h"
#include "message/server_status_message.h"
#include "message/client_cmd_message.h"
#include "message/message_factory.h"
#include "common/base_packet_streamer.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "new_client/fsname.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/tfs_rc_client_api_impl.h"
#include "new_client/tfs_meta_client_api_impl.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

static TfsClientImpl* g_tfs_client = NULL;
static STR_FUNC_MAP g_cmd_map;
static int64_t app_id = 1;
static int64_t uid = 1234;
static const char* dev_name = "bond0";
static const char* app_ip = tbsys::CNetUtil::addrToString(static_cast<uint64_t>(tbsys::CNetUtil::getLocalAddr(dev_name))).c_str();
static const char* default_app_key = "tfscom";
char app_key[256];
//NameMetaClientImpl impl;


#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

char* match_cmd(const char* text, int state)
{
  static STR_FUNC_MAP_ITER it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = g_cmd_map.begin();
    len = strlen(text);
  }

  while(it != g_cmd_map.end())
  {
    cmd = it->first.c_str();
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

char** tfscmd_completion (const char* text, int start, int)
{
  rl_attempted_completion_over = 1;
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

static void sign_handler(const int32_t sig);
static void usage(const char* name);
void init();
int main_loop();
int do_cmd(char* buffer);

int put_file_ex(const VSTRING& param, const bool unique, const bool is_large = false);
int put_file_raw_ex(const VSTRING& param, const bool is_large = false);
int remove_file_ex(const VSTRING& param, const int32_t unique);
int remove_file_raw_ex(const VSTRING& param, TfsUnlinkType type);

int cmd_cd(const VSTRING& param);
int cmd_ls(const VSTRING& param);
int cmd_pwd(const VSTRING& param);
int cmd_show_help(const VSTRING& param);
int cmd_quit(const VSTRING& param);
int cmd_put_file(const VSTRING& param);
int cmd_uput_file(const VSTRING& param);
int cmd_put_large_file(const VSTRING& param);
int cmd_get_file(const VSTRING& param);
int cmd_remove_file(const VSTRING& param);
int cmd_uremove_file(const VSTRING& param);
int cmd_undel_file(const VSTRING& param);
int cmd_hide_file(const VSTRING& param);
int cmd_rename_file(const VSTRING& param);
int cmd_stat_file(const VSTRING& param);
int cmd_stat_blk(const VSTRING& param);
int cmd_visit_count_blk(const VSTRING& param);
int cmd_list_file_info(const VSTRING& param);
int cmd_batch_file(const VSTRING& param);
int cmd_check_file_info(const VSTRING& param);
int cmd_list_block(const VSTRING& param);

//for meta server

//the function of raw tfs
int cmd_put_file_raw(const VSTRING& param);
int cmd_put_large_file_raw(const VSTRING& param);
int cmd_get_file_raw(const VSTRING& param);
int cmd_stat_file_raw(const VSTRING& param);
int cmd_remove_file_raw(const VSTRING& param);
int cmd_unremove_file_raw(const VSTRING& param);
int cmd_hide_file_raw(const VSTRING& param);

int cmd_stat_file_meta(const VSTRING& param);
int cmd_ls_dir_meta(const VSTRING& param);
int cmd_ls_file_meta(const VSTRING& param);
int cmd_create_dir_meta(const VSTRING& param);
int cmd_create_file_meta(const VSTRING& param);
int cmd_rm_dir_meta(const VSTRING& param);
int cmd_rm_file_meta(const VSTRING& param);
int cmd_put_file_meta(const VSTRING& param);
int cmd_get_file_meta(const VSTRING& param);
int cmd_is_dir_exist_meta(const VSTRING& param);
int cmd_is_file_exist_meta(const VSTRING& param);

const char* rc_addr = NULL;
const char* nsip = NULL;
bool g_use_meta = false;

static tfs::common::BasePacketStreamer gstreamer;
static tfs::message::MessageFactory gfactory;
int main(int argc, char* argv[])
{
  int32_t i;
  int ret = TFS_SUCCESS;
  bool directly = false;
  bool set_log_level = false;

  // analyze arguments
  while ((i = getopt(argc, argv, "s:r:nih")) != EOF)
  {
    switch (i)
    {
      case 'n':
        set_log_level = true;
        break;
      case 's':
        nsip = optarg;
        break;
      case 'r':
        rc_addr = optarg;
        break;
      case 'i':
        directly = true;
        break;
      case 'h':
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  if (NULL == nsip && NULL == rc_addr)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  gstreamer.set_packet_factory(&gfactory);
  NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  if (nsip != NULL)
  {
    g_tfs_client = TfsClientImpl::Instance();
    ret = g_tfs_client->initialize(nsip, DEFAULT_BLOCK_CACHE_TIME, 1000, false);
    if (ret != TFS_SUCCESS)
    {
      fprintf(stderr, "init tfs client fail, ret: %d\n", ret);
      return ret;
    }
    g_use_meta = false;
  }
  else if (rc_addr != NULL)
  {
    strcpy(app_key, default_app_key);
    g_use_meta = true;
  }


  init();

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else // has other params
  {
    int32_t i = 0;
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        do_cmd(argv[i]);
      }
    }
    else
    {
      VSTRING param;
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
        cmd_batch_file(param);
      }
    }
  }
  if (g_tfs_client != NULL)
  {
    g_tfs_client->destroy();
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr,
          "Usage: a) %s -s nsip [-n] [-i] [-h] raw tfs client interface(without rc). \n"
          "       b) %s -r rcip [-n] [-i] [-h] name meta client interface(with rc). \n"
          "       -s nameserver ip port\n"
          "       -r rcserver ip port\n"
          "       -n set log level\n"
          "       -i directly execute the command\n"
          "       -h help\n",
          name, name);
}

static void sign_handler(const int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "\nTFS> ");
      break;
  }
}

void init()
{
  g_cmd_map["help"] = CmdNode("help", "show help info", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit", "quit", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit", "exit", 0, 0, cmd_quit);
  g_cmd_map["@"] = CmdNode("@ file", "batch run command in file", 1, 1, cmd_batch_file);
  g_cmd_map["batch"] = CmdNode("batch file", "batch run command in file", 1, 1, cmd_batch_file);
  if (!g_use_meta)
  {
    g_cmd_map["cd"] = CmdNode("cd [directory]", "change work directory", 0, 1, cmd_cd);
    g_cmd_map["ls"] = CmdNode("ls [directory]", "list directory content", 0, 1, cmd_ls);
    g_cmd_map["pwd"] = CmdNode("pwd", "print current directory", 0, 0, cmd_pwd);
    g_cmd_map["put"] = CmdNode("put localfile [tfsname [suffix] [force]]", "put file to tfs", 1, 4, cmd_put_file);
    g_cmd_map["uput"] = CmdNode("uput localfile [tfsname [suffix] [force]]", "unique put file to tfs", 1, 4, cmd_uput_file);
    // put large file not support update now
    g_cmd_map["putl"] = CmdNode("putl localfile [suffix]", "put file to tfs large file", 1, 2, cmd_put_large_file);
    g_cmd_map["get"] = CmdNode("get tfsname localfile", "get file from tfs", 2, 2, cmd_get_file);
    g_cmd_map["rm"] = CmdNode("rm tfsname", "remove tfs file", 1, 1, cmd_remove_file);
    g_cmd_map["urm"] = CmdNode("urm tfsname", "unique remove tfs file", 1, 1, cmd_uremove_file);
    g_cmd_map["undel"] = CmdNode("undel tfsname", "undelete tfs file", 1, 1, cmd_undel_file);
    g_cmd_map["hide"] = CmdNode("hide tfsname [action]", "hide tfs file", 1, 2, cmd_hide_file);
    g_cmd_map["rename"] = CmdNode("rename tfsname newsuffix", "rename tfs file to new suffix", 2, 2, cmd_rename_file);
    g_cmd_map["stat"] = CmdNode("stat tfsname", "stat tfs file", 1, 1, cmd_stat_file);
    g_cmd_map["statblk"] = CmdNode("statblk blockid [serverip:port]", "stat a block", 1, 2, cmd_stat_blk);
    g_cmd_map["vcblk"] = CmdNode("vcblk serverip:port count", "visit count block", 2, 2, cmd_visit_count_blk);
    g_cmd_map["lsf"] = CmdNode("lsf blockid [detail] [serverip:port]" , "list file list in block", 1, 3, cmd_list_file_info);
    g_cmd_map["listblock"] = CmdNode("listblock blockid", "list block server list", 1, 1, cmd_list_block);
    g_cmd_map["cfi"] = CmdNode("cfi tfsname", "check file info", 1, 1, cmd_check_file_info);
  }
  else
  {
    g_cmd_map["put"] = CmdNode("put localfile [[suffix] [app_key]]", "put raw file to tfs", 1, 3, cmd_put_file_raw);
    g_cmd_map["putl"] = CmdNode("putl localfile [suffix [app_key]]", "put raw file to tfs large file", 1, 3, cmd_put_large_file_raw);
    g_cmd_map["get"] = CmdNode("get tfsname localfile [app_key]", "get raw file from tfs", 2, 3, cmd_get_file_raw);
    g_cmd_map["stat"] = CmdNode("stat tfsname [app_key]", "stat raw tfs file", 1, 2, cmd_stat_file_raw);
    g_cmd_map["rm"] = CmdNode("rm tfsname [app_key]", "remove raw tfs file", 1, 2, cmd_remove_file_raw);
    g_cmd_map["undel"] = CmdNode("undel tfsname [app_key]", "undelete raw tfs file", 1, 2, cmd_unremove_file_raw);
    g_cmd_map["hide"] = CmdNode("hide tfsname [action [app_key]]", "hide raw tfs file, param 4 for hide and 6 for unhide", 1, 3, cmd_hide_file_raw);


    g_cmd_map["stat_file_meta"] = CmdNode("stat_file_meta full_path_file_name rootserver_addr [app_id uid], optional param should be in order",
        "get fragment info for meta file", 2, 4, cmd_stat_file_meta);
    g_cmd_map["ls_dir_meta"] = CmdNode("ls_dir_meta full_path_dir_name [ app_key app_id uid ], optional param should be in order",
        "ls files and dirs in full_path_dir_name", 1, 4, cmd_ls_dir_meta);
    g_cmd_map["ls_file_meta"] = CmdNode("ls_file_meta full_path_file_name [ app_key app_id uid ], optional param should be in order",
        "ls file info and frag infos of full_path_file_name", 1, 4, cmd_ls_file_meta);
    g_cmd_map["create_dir_meta"] = CmdNode("create_dir_meta full_path_dir_name [ app_key uid ], optional param should be in order",
        "create full_path_dir_name", 1, 3, cmd_create_dir_meta);
    g_cmd_map["create_file_meta"] = CmdNode("create_file_meta full_path_file_name [ app_key uid ], optional param should be in order",
        "create full_path_file_name", 1, 3, cmd_create_file_meta);
    g_cmd_map["rm_dir_meta"] = CmdNode("rm_dir_meta full_path_dir_name [ app_key uid ], optional param should be in order",
        "rm full_path_dir_name", 1, 3, cmd_rm_dir_meta);
    g_cmd_map["rm_file_meta"] = CmdNode("rm_file_meta full_path_file_name [ app_key uid ], optional param should be in order",
        "rm full_path_file_name", 1, 3, cmd_rm_file_meta);
    g_cmd_map["put_meta"] = CmdNode("put_meta localfile remotefile [ app_key app_id uid ], optional param should be in order",
        "put localfile to remotefile", 2, 5, cmd_put_file_meta);
    g_cmd_map["get_meta"] = CmdNode("get_meta remotefile localfile [ app_key app_id uid ], optional param should be in order",
        "get remotefile to localfile", 2, 5, cmd_get_file_meta);
    g_cmd_map["is_dir_exist_meta"] = CmdNode("is_dir_exist_meta full_path_dir_name [ app_key app_id uid ], optional param should be in order",
        "check if dir exist", 1, 4, cmd_is_dir_exist_meta);
    g_cmd_map["is_file_exist_meta"] = CmdNode("is_file_exist_meta full_path_file_name [ app_key app_id uid ], optional param should be in order",
        "check if file exist", 1, 4, cmd_is_file_exist_meta);
  }
}

int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = tfscmd_completion;
#else
  char cmd_line[MAX_CMD_SIZE];
#endif

  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("TFS> ");
    if (!cmd_line)
#else
      fprintf(stdout, "TFS> ");
    if (NULL == fgets(cmd_line, MAX_CMD_SIZE, stdin))
#endif
    {
      continue;
    }
    ret = do_cmd(cmd_line);
#ifdef _WITH_READ_LINE
    free(cmd_line);
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
  char* token;
  while (' ' == *key)
  {
    key++;
  }
  token = key + strlen(key);
  while (' ' == *(token - 1) || '\n' == *(token - 1) || '\r' == *(token - 1))
  {
    token--;
  }
  *token = '\0';
  if ('\0' == key[0])
  {
    return TFS_SUCCESS;
  }

#ifdef _WITH_READ_LINE
  // not blank line, add to history
  add_history(key);
#endif

  token = strchr(key, ' ');
  if (token != NULL)
  {
    *token = '\0';
  }

  string cur_cmd = Func::str_to_lower(key);
  STR_FUNC_MAP_ITER it = g_cmd_map.find(cur_cmd);

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

  VSTRING param;
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if ('\0' == token[0])
    {
      continue;
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

const char* canonical_param(const string& param)
{
  const char* ret_param = param.c_str();
  if (NULL != ret_param &&
      (strlen(ret_param) == 0 ||
       strcasecmp(ret_param, "null") == 0))
  {
    ret_param = NULL;
  }
  return ret_param;
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

int put_file_ex(const VSTRING& param, const bool unique, const bool is_large)
{
  int32_t size = param.size();
  const char* local_file = expand_path(const_cast<string&>(param[0]));
  const char* tfs_name = NULL;
  const char* suffix = NULL;
  int32_t flag = T_DEFAULT;
  int ret = TFS_SUCCESS;
  char ret_tfs_name[TFS_FILE_LEN];
  ret_tfs_name[0] = '\0';


  if (is_large)
  {
    if (size > 1)
    {
      suffix = canonical_param(param[1]);
    }
    flag |= T_LARGE;
  }
  else
  {
    if (size > 1)
    {
      tfs_name = canonical_param(param[1]);
    }

    if (size > 2)
    {
      suffix = canonical_param(param[2]);
    }

    if (size > 3 &&
        param[3] == "force")
    {
      flag |= T_NEWBLK;
    }
  }

  if (unique)
  {
    // TODO: save unique
    if (tfs_name != NULL)
    {
      ret = g_tfs_client->save_file_update(local_file, flag, tfs_name, suffix) < 0 ? TFS_ERROR : TFS_SUCCESS;
    }
    else
    {
      ret = g_tfs_client->save_file(ret_tfs_name, TFS_FILE_LEN, local_file, flag, suffix) < 0 ?
          TFS_ERROR : TFS_SUCCESS;
    }
  }
  else
  {
    if (tfs_name != NULL)
    {
      ret = g_tfs_client->save_file_update(local_file, flag, tfs_name, suffix) < 0 ? TFS_ERROR : TFS_SUCCESS;
    }
    else
    {
      ret = g_tfs_client->save_file(ret_tfs_name, TFS_FILE_LEN, local_file, flag, suffix) < 0 ?
          TFS_ERROR : TFS_SUCCESS;
    }
  }

  //printf("tfs_name: %s, ret_tfs_name: %s\n", tfs_name, ret_tfs_name);
  ToolUtil::print_info(ret, "put %s => %s", local_file, tfs_name != NULL ? FSName(tfs_name, suffix).get_name() : ret_tfs_name);
  return ret;
}

int put_file_raw_ex(const VSTRING& param, const bool is_large)
{
  int32_t size = param.size();
  const char* local_file = expand_path(const_cast<string&>(param[0]));
  char* tfs_name = NULL;
  const char* suffix = NULL;
  char appkey[257];
  //int32_t flag = T_DEFAULT;
  int ret = TFS_SUCCESS;
  char ret_tfs_name[TFS_FILE_LEN];
  ret_tfs_name[0] = '\0';

  if (size > 1)
  {
    suffix = canonical_param(param[1]);
  }

  if (size > 2)
  {
    //TBSYS_LOG(DEBUG, "app_key: %s", param[2].c_str());
    strncpy(appkey, canonical_param(param[2]), 256);
    appkey[256] = '\0';
  }
  else
  {
    // default app_key = "tfscom"
    strcpy(appkey, app_key);
  }

  //login with rc and app_key
  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "rc client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.save_file(local_file, ret_tfs_name, TFS_FILE_LEN, suffix, is_large) < 0 ? TFS_ERROR : TFS_SUCCESS;
  }

  //printf("tfs_name: %s, ret_tfs_name: %s\n", tfs_name, ret_tfs_name);

  ToolUtil::print_info(ret, "put %s => %s", local_file, tfs_name != NULL ? FSName(tfs_name, suffix).get_name() : ret_tfs_name);
  return ret;
}



int remove_file_ex(const VSTRING& param, const int32_t unique)
{
  const char* tfs_name = canonical_param(param[0]);
  int ret = TFS_SUCCESS;
  int64_t file_size = 0;
  if (unique)
  {
    // TODO: unlink_unique
    ret = g_tfs_client->unlink(file_size, tfs_name, NULL);
  }
  else
  {
    ret = g_tfs_client->unlink(file_size, tfs_name, NULL);
  }

  ToolUtil::print_info(ret, "unlink %s", tfs_name);

  return ret;
}

int remove_file_raw_ex(const VSTRING& param, TfsUnlinkType type)
{
  const char* tfs_name = canonical_param(param[0]);
  char appkey[257];
  int ret = TFS_SUCCESS;


  int size = param.size();
  if (size > 1)
  {
    //TBSYS_LOG(DEBUG, "app_key: %s", param[1].c_str());
    strncpy(appkey, canonical_param(param[1]), 256);
    appkey[256] = '\0';
  }
  else
  {
    //default app_key = "tfscom"
    strcpy(appkey, app_key);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "rc client init fail, ret: %d", ret);
  }
  else
  {
    ret = impl.unlink(tfs_name, NULL, type);
  }

  if (type == DELETE)
  {
    ToolUtil::print_info(ret, "del %s", tfs_name);
  }
  else if (type == UNDELETE)
  {
    ToolUtil::print_info(ret, "undel %s", tfs_name);
  }

  return ret;
}



int cmd_ls(const VSTRING& param)
{
  int32_t size = param.size();
  const char* path = (1 == size) ? param.at(0).c_str() : ".";
  char sys_cmd[MAX_CMD_SIZE];
  // just use system tool ls, maybe DIY
  snprintf(sys_cmd, MAX_CMD_SIZE, "ls -FCl %s", path);
  return system(sys_cmd);
}

int cmd_pwd(const VSTRING&)
{
  char dir[MAX_PATH_LENGTH], *path = NULL;

  path = getcwd(dir, MAX_PATH_LENGTH - 1);
  if (NULL == path)
  {
    fprintf (stderr, "can't get current work directory: %s\n", strerror(errno));
    return TFS_ERROR;
  }
  fprintf(stdout, "%s\n", dir);
  return TFS_SUCCESS;
}

int cmd_cd(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  int32_t size = param.size();
  const char* dest_dir = (1 == size) ? expand_path(const_cast<string&>(param[0])) : getenv("HOME");

  if (NULL == dest_dir)
  {
    fprintf(stderr, "no directory argument and HOME not found\n\n");
    ret = TFS_ERROR;
  }
  else if (chdir(dest_dir) == -1)
  {
    fprintf(stderr, "can't change directory %s: %s\n", dest_dir, strerror(errno));
    ret = TFS_ERROR;
  }
  cmd_pwd(param);
  return ret;
}

int cmd_quit(const VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_show_help(const VSTRING&)
{
  return ToolUtil::show_help(g_cmd_map);
}

int cmd_put_file(const VSTRING& param)
{
  return put_file_ex(param, false);
}

int cmd_uput_file(const VSTRING& param)
{
  return put_file_ex(param, true);
}

int cmd_put_large_file(const VSTRING& param)
{
  return put_file_ex(param, false, true);
}

int cmd_get_file(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);
  const char* local_file = expand_path(const_cast<string&>(param[1]));

  int ret = g_tfs_client->fetch_file(local_file, tfs_name, NULL);

  ToolUtil::print_info(ret, "fetch %s => %s", tfs_name, local_file);

  return ret;
}

int cmd_remove_file(const VSTRING& param)
{
  return remove_file_ex(param, 0);
}

int cmd_uremove_file(const VSTRING& param)
{
  return remove_file_ex(param, 1);
}

int cmd_undel_file(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);
  int64_t file_size = 0;
  int ret = g_tfs_client->unlink(file_size, tfs_name, NULL, UNDELETE);

  ToolUtil::print_info(ret, "undel %s", tfs_name);

  return TFS_SUCCESS;
}

int cmd_hide_file(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);

  TfsUnlinkType unlink_type = CONCEAL;
  if (param.size() > 1)
  {
    unlink_type = static_cast<TfsUnlinkType>(atoi(param[1].c_str()));
  }

  int64_t file_size = 0;
  int ret = g_tfs_client->unlink(file_size, tfs_name, NULL, unlink_type);

  ToolUtil::print_info(ret, "hide %s %d", tfs_name, unlink_type);

  return ret;
}

int cmd_rename_file(const VSTRING&)
{
  // TODO: rename
  // const char* tfs_name = canonical_param(param[0]);
  // const char* new_suffix = canonical_param(param[1]);
  // int ret = tfs_client->rename(tfs_name, NULL, new_suffix);

  // print_info(ret, "rename %s to new suffix %s", tfs_name, new_suffix);

  // return ret;
  return TFS_SUCCESS;
}

int cmd_stat_file(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);

  TfsFileStat file_stat;
  int ret = g_tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);

  ToolUtil::print_info(ret, "stat %s", tfs_name);

  if (TFS_SUCCESS == ret)
  {
    FSName fsname(tfs_name, NULL);
    fprintf(stdout,
            "  FILE_NAME:     %s\n"
            "  BLOCK_ID:      %u\n"
            "  FILE_ID:       %" PRI64_PREFIX "u\n"
            "  OFFSET:        %d\n"
            "  SIZE:          %"PRI64_PREFIX"d\n"
            "  OCCUPY SIZE:   %"PRI64_PREFIX"d\n"
            "  MODIFIED_TIME: %s\n"
            "  CREATE_TIME:   %s\n"
            "  STATUS:        %d\n"
            "  CRC:           %u\n",
            tfs_name, fsname.get_block_id(), file_stat.file_id_,
            file_stat.offset_, file_stat.size_, file_stat.usize_,
            Func::time_to_str(file_stat.modify_time_).c_str(),
            Func::time_to_str(file_stat.create_time_).c_str(),
            file_stat.flag_, file_stat.crc_);
  }

  // uint64_t id = file_stat.size_;
  // id <<= 32;
  // id |= file_info.crc_;
  //  uint32_t tindex = static_cast<uint32_t>(id & 0xFFFFFFFF);
  //  printf("  TABLE:         select * from t%d where id='%" PRI64_PREFIX "u'\n", tindex % TABLE_COUNT, id);

  return ret;
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

int cmd_stat_blk(const VSTRING& param)
{
  int ret = TFS_ERROR;

  uint64_t server_id = 0;
  uint32_t block_id = 0;

  if ((block_id = atoi(param[0].c_str())) <= 0)
  {
    fprintf(stderr, "invalid blockid: %u\n", block_id);
  }

  if (param.size() > 2)
  {
    server_id = Func::get_host_ip(param[1].c_str());
  }
  else
  {
    VUINT64 ds_list;
    ret = ToolUtil::get_block_ds_list(g_tfs_client->get_server_id(), block_id, ds_list);
    if (ret != TFS_SUCCESS)
    {
      fprintf(stderr, "get ds list failed. block_id: %u, ret: %d\n", block_id, ret);
      return ret;
    }
    server_id = ds_list[0];
  }

  DsTask ds_task(server_id, g_tfs_client->get_cluster_id());
  ds_task.block_id_ = block_id;
  ret = DsLib::get_block_info(ds_task);

  return ret;
}

int cmd_visit_count_blk(const VSTRING& param)
{
  uint64_t server_id = 0;
  int32_t count = 0;
  int ret = TFS_ERROR;

  if ((server_id  = Func::get_host_ip(param[0].c_str())) <= 0)
  {
    fprintf(stderr, "invalid ds address: %s\n", param[0].c_str());
  }
  else if ((count = atoi(param[1].c_str())) <= 0)
  {
    fprintf(stderr, "invalid count: %d\n", count);
  }
  else
  {
    DsTask ds_task(server_id, g_tfs_client->get_cluster_id());
    ds_task.num_row_ = count;
    ret = DsLib::get_server_status(ds_task);
  }
  return ret;
}

int cmd_list_file_info(const VSTRING& param)
{
  uint32_t block_id = 0;
  uint64_t server_id = 0;

  int32_t show_detail = 0;

  int ret = TFS_ERROR;
  if ((block_id = atoi(param[0].c_str())) <= 0)
  {
    fprintf(stderr, "invalid blockid: %u\n", block_id);
    return ret;
  }

  if (param.size() > 2)
  {
    server_id = Func::get_host_ip(param[2].c_str());
  }
  else
  {
    VUINT64 ds_list;
    ret = ToolUtil::get_block_ds_list(g_tfs_client->get_server_id(), block_id, ds_list);
    if (ret != TFS_SUCCESS)
    {
      fprintf(stderr, "get ds list failed. block_id: %u, ret: %d\n", block_id, ret);
      return ret;
    }
    server_id = ds_list[0];
  }

  if (0 != server_id)
  {
    if (param.size() > 1 && 0 == strcmp(param[1].c_str(), "detail"))
    {
      show_detail = 1;
    }
    DsTask ds_task(server_id, g_tfs_client->get_cluster_id());
    ds_task.block_id_ = block_id;
    ds_task.mode_ = show_detail;
    ret = DsLib::list_file(ds_task);
  }

  return ret;
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

int cmd_check_file_info(const VSTRING& param)
{
  int ret = TFS_ERROR;
  FSName fsname(canonical_param(param[0]));

  if (!fsname.is_valid())
  {
    fprintf(stderr, "file name is invalid: %s\n", param[0].c_str());
  }
  else
  {
    VUINT64 ds_list;
    ret = ToolUtil::get_block_ds_list(g_tfs_client->get_server_id(), fsname.get_block_id(), ds_list);
    if (ret != TFS_SUCCESS)
    {
      fprintf(stderr, "get block info fail, ret: %d\n", ret);
    }
    else
    {
      DsTask ds_task(0, g_tfs_client->get_cluster_id());
      int32_t ds_size = ds_list.size();
      for (int32_t i = 0; i < ds_size; ++i)
      {
        ds_task.server_id_ = ds_list[i];
        ds_task.block_id_ = fsname.get_block_id();
        ds_task.new_file_id_ = fsname.get_file_id();
        ret = DsLib::check_file_info(ds_task);
      }
    }
  }
  return ret;
}

int cmd_stat_file_raw(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);
  char appkey[257];
  int size = param.size();

  if (size > 1)
  {
    //TBSYS_LOG(DEBUG, "app_key: %s", param[1].c_str());
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  TfsFileStat file_stat;

  RcClientImpl impl;
  int ret = impl.initialize(rc_addr, appkey, app_ip);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "rc init fail, ret: %d", ret);
  }
  else
  {
    int fd = -1;
    if (tfs_name[0] == 'L')
    {
      fd = impl.open(tfs_name, NULL, RcClient::READ, true);
    }
    else
    {
      fd = impl.open(tfs_name, NULL, RcClient::READ);
    }
    if (fd < 0)
    {
      ret = TFS_ERROR;
      TBSYS_LOG(DEBUG, "%s open fail, return fd: %d", tfs_name, fd);
    }
    else
    {
      if ((ret = impl.fstat(fd, &file_stat, FORCE_STAT)) != TFS_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "stat %s fail, return %d", tfs_name, ret);
      }
      int re = impl.close(fd, const_cast<char*>(tfs_name), TFS_FILE_LEN);
      if (TFS_SUCCESS != re)
      {
        TBSYS_LOG(DEBUG, "close %s fail, return %d", tfs_name, re);
      }
    }
  }

  ToolUtil::print_info(ret, "stat %s", tfs_name);

  if (TFS_SUCCESS == ret)
  {
    FSName fsname(tfs_name, NULL);
    fprintf(stdout,
            "  FILE_NAME:     %s\n"
            "  BLOCK_ID:      %u\n"
            "  FILE_ID:       %" PRI64_PREFIX "u\n"
            "  OFFSET:        %d\n"
            "  SIZE:          %"PRI64_PREFIX"d\n"
            "  OCCUPY SIZE:   %"PRI64_PREFIX"d\n"
            "  MODIFIED_TIME: %s\n"
            "  CREATE_TIME:   %s\n"
            "  STATUS:        %d\n"
            "  CRC:           %u\n",
            tfs_name, fsname.get_block_id(), file_stat.file_id_,
            file_stat.offset_, file_stat.size_, file_stat.usize_,
            Func::time_to_str(file_stat.modify_time_).c_str(),
            Func::time_to_str(file_stat.create_time_).c_str(),
            file_stat.flag_, file_stat.crc_);
  }

  // uint64_t id = file_stat.size_;
  // id <<= 32;
  // id |= file_info.crc_;
  //  uint32_t tindex = static_cast<uint32_t>(id & 0xFFFFFFFF);
  //  printf("  TABLE:         select * from t%d where id='%" PRI64_PREFIX "u'\n", tindex % TABLE_COUNT, id);

  return ret;
}

int cmd_stat_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* file_path = expand_path(const_cast<string&>(param[0]));
  char rs_addr[257];
  FragInfo frag_info_;

  int size = param.size();
  if (size > 1)
  {
    strncpy(rs_addr, param[1].c_str(), 256);
    rs_addr[256] = '\0';
    printf("rs_addr: %s\n", rs_addr);
  }

  if (size > 2)
  {
    app_id = strtoll(param[2].c_str(), NULL, 10);
  }
  if (size > 3)
  {
    uid = strtoll(param[3].c_str(), NULL, 10);
  }

  NameMetaClientImpl impl;
  ret = impl.initialize(rs_addr);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client login rs_addr: %s fail, ret: %d", rs_addr, ret);
  }
  else
  {
    ret = impl.read_frag_info(app_id, uid, file_path, frag_info_);
  }

  //stat the detail
  ToolUtil::print_info(ret, "stat %s", file_path);
  if (TFS_SUCCESS == ret)
  {
    int32_t cluster_id = frag_info_.cluster_id_;
    std::vector<FragMeta> vFragMeta = frag_info_.v_frag_meta_;
    std::vector<FragMeta>::iterator iter;
    fprintf(stdout, "  FILE_NAME\t  BLOCK_ID\t  FILE_ID\t  OFFSET\t  SIZE\n");
    for (iter = vFragMeta.begin(); iter < vFragMeta.end(); iter++)
    {
      FSName fsname(iter->block_id_, iter->file_id_, cluster_id);
      fprintf(stdout,
            "  %s\t"
            "  %u\t"
            "  %" PRI64_PREFIX "u\t"
            "  %"PRI64_PREFIX"d\t"
            "  %d\n",
            fsname.get_name(), iter->block_id_, iter->file_id_, iter->offset_, iter->size_);
    }
  }

  return ret;
}

int cmd_ls_dir_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* dir_path = expand_path(const_cast<string&>(param[0]));
  int size = param.size();
  char appkey[257];
  TBSYS_LOG(DEBUG, "size: %d", size);
  if (size > 1)
  {
    TBSYS_LOG(DEBUG, "appkey: %s", param[1].c_str());
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    app_id = strtoll(param[2].c_str(), NULL, 10);
  }
  if (size > 3)
  {
    uid = strtoll(param[3].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    std::vector<FileMetaInfo> meta_info;
    std::vector<FileMetaInfo>::const_iterator it;
    ret = impl.ls_dir(app_id, uid, dir_path, meta_info);
    if (TFS_SUCCESS == ret)
    {
      for (it = meta_info.begin(); it != meta_info.end(); it++)
      {
        if (it->name_.size() > 0)
          fprintf(stdout, "name:%s\n", it->name_.data());
        fprintf(stdout, "pid %"PRI64_PREFIX"d id %"PRI64_PREFIX
            "d create_time %d modify_time %d size %"PRI64_PREFIX"d ver_no %d\n",
            it->pid_, it->id_, it->create_time_, it->modify_time_, it->size_, it->ver_no_);
      }
    }
  }
  return ret;
}


int cmd_ls_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  char appkey[257];
  if (!g_use_meta)
  {
    TBSYS_LOG(WARN, "sorry, this commond is for name meta!!!");
  }
  else
  {
    const char* file_path = expand_path(const_cast<string&>(param[0]));
    int size = param.size();
    if (size > 1)
    {
      strncpy(appkey, param[1].c_str(), 256);
      appkey[256] = '\0';
    }
    else
    {
      strcpy(appkey, app_key);
    }

    if (size > 2)
    {
      app_id = strtoll(param[2].c_str(), NULL, 10);
    }
    if (size > 3)
    {
      uid = strtoll(param[3].c_str(), NULL, 10);
    }

    RcClientImpl impl;
    ret = impl.initialize(rc_addr, appkey, app_ip);

    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
    }
    else
    {
      FileMetaInfo file_info;
      ret = impl.ls_file(app_id, uid, file_path, file_info);
      if (TFS_SUCCESS == ret)
      {
        if (file_info.name_.size() > 0)
          fprintf(stdout, "name:%s\n", file_info.name_.data());
        fprintf(stdout, "pid %"PRI64_PREFIX"d id %"PRI64_PREFIX
            "d create_time %s modify_time %s size %"PRI64_PREFIX"d ver_no %d\n",
            file_info.pid_, file_info.id_, Func::time_to_str(file_info.create_time_).c_str(),
            Func::time_to_str(file_info.modify_time_).c_str(), file_info.size_, file_info.ver_no_);
      }
    }
  }
  return ret;
}

int cmd_create_dir_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  char appkey[257];
  const char* dir_path = expand_path(const_cast<string&>(param[0]));
  int size = param.size();
  if (size > 1)
  {
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    uid = strtoll(param[2].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.create_dir_with_parents(uid, dir_path);
  }
  return ret;
}

int cmd_create_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* file_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  if (size > 1)
  {
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    uid = strtoll(param[2].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.create_file(uid, file_path);
  }
  return ret;
}

int cmd_rm_dir_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* dir_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  if (size > 1)
  {
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    uid = strtoll(param[2].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.rm_dir(uid, dir_path);
  }

  return ret;
}

int cmd_remove_file_raw(const VSTRING& param)
{
  return remove_file_raw_ex(param, static_cast<TfsUnlinkType>(0));
}

int cmd_unremove_file_raw(const VSTRING& param)
{
  return remove_file_raw_ex(param, static_cast<TfsUnlinkType>(2));
}

int cmd_hide_file_raw(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);
  char appkey[257];
  int size = param.size();

  TfsUnlinkType unlink_type = CONCEAL;
  if (size > 1)
  {
    unlink_type = static_cast<TfsUnlinkType>(atoi(param[1].c_str()));
  }

  if (size > 2)
  {
    strncpy(appkey, param[2].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  RcClientImpl impl;
  int ret = impl.initialize(rc_addr, appkey, app_ip);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "rc client login fail, ret: %d", ret);
  }
  else
  {
    ret = impl.unlink(tfs_name, NULL, unlink_type);
  }

  ToolUtil::print_info(ret, "hide %s %d", tfs_name, unlink_type);

  return ret;
}

int cmd_rm_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* file_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  if (size > 1)
  {
    //TBSYS_LOG(DEBUG, "app_key: %s", param[1].c_str());
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    uid = strtoll(param[2].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.rm_file(uid, file_path);
  }

  return ret;
}

int cmd_put_file_raw(const VSTRING& param)
{
   return put_file_raw_ex(param);
}

int cmd_put_large_file_raw(const VSTRING& param)
{
  return put_file_raw_ex(param, true);
}


int cmd_put_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* local_file = expand_path(const_cast<string&>(param[0]));
  const char* file_path = expand_path(const_cast<string&>(param[1]));
  char appkey[257];
  int size = param.size();
  if (size > 2)
  {
    strncpy(appkey, param[2].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 3)
  {
    app_id = strtoll(param[3].c_str(), NULL, 10);
  }
  if (size > 4)
  {
    uid = strtoll(param[4].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    FILE* fp=fopen(local_file,"r");
    if(fp == NULL)
    {
      TBSYS_LOG(WARN, "open local file failed. local_file: %s, ret: %p", local_file, fp);
      ret = TFS_ERROR;
    }
    else
    {
      // create file if not exist
      FileMetaInfo file_meta_info;
      ret = impl.ls_file(app_id, uid, file_path, file_meta_info);
      if (ret != TFS_SUCCESS)
      {
        ret = impl.create_file(uid, file_path);
      }

      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "create file failed. file_path: %s, ret: %d", file_path, ret);
      }
      else
      {
        int fd = impl.open(app_id, uid, file_path, RcClient::WRITE);
        if (fd < 0)
        {
          TBSYS_LOG(WARN, "open file path failed. file_path: %s, ret: %d", file_path, fd);
          ret = TFS_ERROR;
        }
        else
        {
          int64_t offset = 0;
          char buffer[MAX_READ_SIZE];
          while (true)
          {
            int64_t rlen = fread(buffer,sizeof(char),MAX_READ_SIZE,fp);
            if (rlen <= 0)
            {
              break;
            }

            int64_t wlen = impl.pwrite(fd, buffer, rlen, offset);
            if (wlen != rlen)
            {
              TBSYS_LOG(ERROR, "write meta data failed. expect len: %"PRI64_PREFIX"d, return len: %"PRI64_PREFIX"d", rlen, wlen);
              ret = TFS_ERROR;
              break;
            }
            offset = -1;
            if (rlen < MAX_READ_SIZE)
            {
              break;
            }
          }
          impl.close(fd);
        }
      }
      fclose(fp);
    }
  }

  ToolUtil::print_info(ret, "put %s => %s", local_file, file_path);
  return ret;
}

int cmd_get_file_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* local_file = expand_path(const_cast<string&>(param[1]));
  const char* file_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  if (size > 2)
  {
    strncpy(appkey, param[2].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 3)
  {
    app_id = strtoll(param[3].c_str(), NULL, 10);
  }
  if (size > 4)
  {
    uid = strtoll(param[4].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    int fd = impl.open(app_id, uid, file_path, RcClient::READ);
    if (fd < 0)
    {
      TBSYS_LOG(WARN, "open file path failed. file_path: %s, ret: %d", file_path, fd);
      ret = TFS_ERROR;
    }
    else
    {
      FILE* fp=fopen(local_file,"w+b");
      if(fp == NULL)
      {
        TBSYS_LOG(WARN, "open local file failed. local_file: %s, ret: %p", local_file, fp);
        ret = TFS_ERROR;
      }
      else
      {
        int64_t offset = 0;
        char buffer[MAX_READ_SIZE];
        while (true)
        {
          int rlen = impl.pread(fd, buffer, MAX_READ_SIZE, offset);
          if (rlen <= 0)
          {
            break;
          }

          offset += rlen;
          int wlen = fwrite(buffer,sizeof(char),rlen,fp);
          if (wlen != rlen)
          {
            TBSYS_LOG(ERROR, "write to local file error, expect len: %d, return len: %d", rlen, wlen);
            ret= TFS_ERROR;
            break;
          }
          if (rlen < MAX_READ_SIZE)
          {
            break;
          }
        }
        fclose(fp);
      }
      impl.close(fd);
    }
  }
  ToolUtil::print_info(ret, "get %s => %s", file_path, local_file);
  return ret;
}

int cmd_get_file_raw(const VSTRING& param)
{
  const char* tfs_name = canonical_param(param[0]);
  const char* local_file = expand_path(const_cast<string&>(param[1]));
  char appkey[257];
  int size = param.size();
  if (size > 2)
  {
    TBSYS_LOG(DEBUG, "appkey: %s", param[2].c_str());
    strncpy(appkey, param[2].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }
  int ret = TFS_SUCCESS;
  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "rc client init failed, ret: %d", ret);
  }
  else
  {
    ret = impl.fetch_file(local_file, tfs_name, NULL);
  }

  ToolUtil::print_info(ret, "fetch %s => %s", tfs_name, local_file);

  return ret;
}

int cmd_is_dir_exist_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* dir_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  TBSYS_LOG(DEBUG, "size: %d", size);
  if (size > 1)
  {
    //TBSYS_LOG(DEBUG, "appkey: %s", param[1].c_str());
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    app_id = strtoll(param[2].c_str(), NULL, 10);
  }
  if (size > 3)
  {
    uid = strtoll(param[3].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    std::vector<FileMetaInfo> meta_info;
    std::vector<FileMetaInfo>::const_iterator it;
    bool bRet = impl.is_dir_exist(app_id, uid, dir_path);
    fprintf(stdout, "dir: %s %s exist \n", dir_path, bRet ? "" : "not ");
  }
  return ret;
}
int cmd_is_file_exist_meta(const VSTRING& param)
{
  int ret = TFS_SUCCESS;
  const char* file_path = expand_path(const_cast<string&>(param[0]));
  char appkey[257];
  int size = param.size();
  TBSYS_LOG(DEBUG, "size: %d", size);
  if (size > 1)
  {
    //TBSYS_LOG(DEBUG, "appkey: %s", param[1].c_str());
    strncpy(appkey, param[1].c_str(), 256);
    appkey[256] = '\0';
  }
  else
  {
    strcpy(appkey, app_key);
  }

  if (size > 2)
  {
    app_id = strtoll(param[2].c_str(), NULL, 10);
  }
  if (size > 3)
  {
    uid = strtoll(param[3].c_str(), NULL, 10);
  }

  RcClientImpl impl;
  ret = impl.initialize(rc_addr, appkey, app_ip);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "meta client init failed, ret: %d", ret);
  }
  else
  {
    bool bRet = impl.is_file_exist(app_id, uid, file_path);
    fprintf(stdout, "file: %s %s exist \n", file_path, bRet ? "" : "not ");
  }
  return ret;
}
