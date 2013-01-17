/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ds_client.cpp 465 2011-06-09 11:26:40Z daoan@taobao.com $
 *
 * Authors:
 *   jihe
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "common/internal.h"
#include "common/func.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tools/util/ds_lib.h"

using namespace tfs::common;
using namespace tfs::tools;
using namespace std;

typedef map<string, int> STR_INT_MAP;
typedef STR_INT_MAP::iterator STR_INT_MAP_ITER;
enum CmdSet
{
  CMD_GET_SERVER_STATUS,
  CMD_GET_PING_STATUS,
  CMD_REMOVE_BlOCK,
  CMD_LIST_BLOCK,
  CMD_GET_BLOCK_INFO,
  CMD_RESET_BLOCK_VERSION,
  CMD_CREATE_FILE_ID,
  CMD_LIST_FILE,
  CMD_READ_FILE_DATA,
  CMD_WRITE_FILE_DATA,
  CMD_UNLINK_FILE,
  CMD_RENAME_FILE,
  CMD_READ_FILE_INFO,
  CMD_SEND_CRC_ERROR,
  CMD_LIST_BITMAP,
  CMD_HELP,
  CMD_UNKNOWN,
  CMD_NOP,
  CMD_QUIT,
};

void init();
void usage(const char* name);
void signal_handler(const int sig);
int parse_cmd(char* buffer, VSTRING & param);
int switch_cmd(const int cmd, VSTRING & param);
int main_loop();
int show_help(VSTRING & param);
int no_operation();

STR_INT_MAP cmd_map;
uint64_t ds_ip = 0;


#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

char* match_cmd(const char* text, int state)
{
  static STR_INT_MAP_ITER it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = cmd_map.begin();
    len = strlen(text);
  }

  while(it != cmd_map.end())
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

char** dscmd_completion (const char* text, int start, int)
{
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

int main(int argc, char* argv[])
{
  int i = 0;
  int iex = 0;

  // input option
  if (argc == 1)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  while ((i = getopt(argc, argv, "d:i::")) != -1)
  {
    switch (i)
    {
    case 'i':
      iex = 1;
      break;
    case 'd':
      ds_ip = Func::get_host_ip(optarg);
      if (ds_ip == 0)
      {
        printf("ip or port is invalid, please try again.\n");
        return TFS_ERROR;
      }
      break;
    case ':':
      printf("missing -d");
      usage(argv[0]);
      break;
    default:
      usage(argv[0]);
      return TFS_ERROR;
    }
  }
  static tfs::message::MessageFactory factory;
  static tfs::common::BasePacketStreamer streamer;
  streamer.set_packet_factory(&factory);
  NewClientManager::get_instance().initialize(&factory, &streamer);

  init();

  if (optind >= argc)
  {
    signal(SIGINT, signal_handler);
    main_loop();
  }
  else
  {
    VSTRING param;
    int i = optind;
    if (iex)
    {
      printf("with i\n");
      for (i = optind; i < argc; i++)
      {
        param.clear();
        int cmd = parse_cmd(argv[i], param);
        switch_cmd(cmd, param);
      }
    }
    else
    {
      printf("without i\n");
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
      }
    }
  }
  return TFS_SUCCESS;
}

// no returns.
// this is initialization for defining the command
void init()
{
  cmd_map["get_server_status"] = CMD_GET_SERVER_STATUS;
  cmd_map["get_ping_status"] = CMD_GET_PING_STATUS;
  cmd_map["remove_block"] = CMD_REMOVE_BlOCK;
  cmd_map["list_block"] = CMD_LIST_BLOCK;
  cmd_map["get_block_info"] = CMD_GET_BLOCK_INFO;
  cmd_map["reset_block_version"] = CMD_RESET_BLOCK_VERSION;
  cmd_map["create_file_id"] = CMD_CREATE_FILE_ID;
  cmd_map["list_file"] = CMD_LIST_FILE;
  cmd_map["read_file_data"] = CMD_READ_FILE_DATA;
  cmd_map["write_file_data"] = CMD_WRITE_FILE_DATA;
  cmd_map["unlink_file"] = CMD_UNLINK_FILE;
  cmd_map["rename_file"] = CMD_RENAME_FILE;
  cmd_map["read_file_info"] = CMD_READ_FILE_INFO;
  cmd_map["help"] = CMD_HELP;
  cmd_map["unknown_cmd"] = CMD_UNKNOWN;
  cmd_map["nop"] = CMD_NOP;
  cmd_map["quit"] = CMD_QUIT;
  cmd_map["send_crc_error"] = CMD_SEND_CRC_ERROR;
  cmd_map["list_bitmap"] = CMD_LIST_BITMAP;
}

// no return.
// show the prompt of command.
void usage(const char* name)
{
  printf("Usage: %s -d ip:port \n", name);
  exit( TFS_ERROR);
}

//no return.
//process the signal
void signal_handler(const int sig)
{
  switch (sig)
  {
  case SIGINT:
    fprintf(stderr, "\nDataServer> ");
    break;
  }
}

//get the command
int parse_cmd(char* key, VSTRING & param)
{
  int cmd = CMD_NOP;
  char* token;
  //remove the space
  while (*key == ' ')
    key++;
  token = key + strlen(key);
  while (*(token - 1) == ' ' || *(token - 1) == '\n' || *(token - 1) == '\r')
    token--;
  *token = '\0';
  if (key[0] == '\0')
  {
    return cmd;
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
  //find the command
  STR_INT_MAP_ITER it = cmd_map.find(Func::str_to_lower(key));
  if (it == cmd_map.end())
  {
    return CMD_UNKNOWN;
  }
  else
  {
    cmd = it->second;
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
  //get the parameters
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if (token[0] == '\0')
    {
      continue;
    }
    param.push_back(token);
  }
  return cmd;
}

int switch_cmd(const int cmd, VSTRING & param)
{
  int ret = TFS_SUCCESS;
  DsTask ds_task(ds_ip);
  switch (cmd)
  {
  case CMD_GET_SERVER_STATUS:
    {
      if (param.size() != 1)
      {
        printf("Usage:get_server_status nums\n");
        printf("get the nums most frequently visited blocks in dataserver.\n");
        break;
      }
      int num_row = atoi(const_cast<char*>(param[0].c_str()));
      ds_task.num_row_ = num_row;
      ret = DsLib::get_server_status(ds_task);
      break;
    }
  case CMD_GET_PING_STATUS:
    {
      if (param.size() != 0)
      {
        printf("Usage:get_ping_status \n");
        printf("get the ping status of dataServer.\n");
        break;
      }
      ret = DsLib::get_ping_status(ds_task);
      break;
    }
  case CMD_REMOVE_BlOCK:
    {
      if (param.size() != 1)
      {
        printf("Usage:remove_block block_id\n");
        printf("delete a block in dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ret = DsLib::remove_block(ds_task);
      break;
    }
  case CMD_LIST_BLOCK:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_block type\n");
        printf("      type  1|2|4,  1          => show all blockid\n\
                                    2          => show the map between logical and physical blockid\n\
                                    4          => show detail info of block on this dataserver\n");
        printf("list all the blocks in a dataserver.\n");
        break;
      }
      int type = atoi(const_cast<char*> (param[0].c_str()));
      ds_task.list_block_type_ = type;
      ret = DsLib::list_block(ds_task);
      break;
    }
  case CMD_GET_BLOCK_INFO:
    {
      if (param.size() != 1)
      {
        printf("Usage:get_block_info block_id\n");
        printf("get the information of a block in the dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ret = DsLib::get_block_info(ds_task);
      break;
    }
  case CMD_RESET_BLOCK_VERSION:
    {
      if (param.size() != 1)
      {
        printf("Usage:rest_block_version block_id\n");
        printf("reset the version of the block in dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ret = DsLib::reset_block_version(ds_task);
      break;
    }
  case CMD_CREATE_FILE_ID:
    {
      if (param.size() != 2)
      {
        printf("Usage:create_file_id block_id file_id\n");
        printf("add a new file_id.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_new_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_new_file_id;
      ret = DsLib::create_file_id(ds_task);
      break;
    }
  case CMD_LIST_FILE:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_file block_id\n");
        printf("list all the files in a block.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ds_task.mode_ = 1;
      ret = DsLib::list_file(ds_task);
      break;
    }
  case CMD_READ_FILE_DATA:
    {
      if (param.size() != 3)
      {
        printf("Usage:read_file_data blockid fileid local_file_name\n");
        printf("download a tfs file to local.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_file_id;
      snprintf(ds_task.local_file_, MAX_PATH_LENGTH, "%s", param[2].c_str());
      ret = DsLib::read_file_data(ds_task);
      if (TFS_SUCCESS == ret)
      {
        printf("download tfs file %u, %"PRI64_PREFIX "u to local file %s success.\n", ds_task.block_id_, ds_task.new_file_id_, ds_task.local_file_);
      }
      else
      {
        printf("download tfs file %u, %"PRI64_PREFIX "u to local file %s fail.\n", ds_task.block_id_, ds_task.new_file_id_, ds_task.local_file_);
      }
      break;
    }
  case CMD_WRITE_FILE_DATA:
    {
      if (param.size() != 3)
      {
        printf("Usage:write_file_data block_id file_id local_file_name\n");
        printf("upload a local file to this dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_file_id;
      snprintf(ds_task.local_file_, MAX_PATH_LENGTH, "%s", param[2].c_str());
      ret = DsLib::write_file_data(ds_task);
      break;
    }
  case CMD_UNLINK_FILE:
    {
      if (param.size() < 2 || param.size() > 5)
      {
        printf("Usage:unlink_file block_id file_id [unlink_type] [option_flag] [is_master]\n");
        printf("      unlink_type  0|2|4|6,  0(default) => delete, 2 => undelete, 4 => conceal, 6 => reveal\n");
        printf("      option_flag  0|1,  0(default) => this unlink action will sync to mirror cluster if is_master is set to 1\n\
                                         1          => this unlink action will not sync to mirror cluster\n");
        printf("      is_master    0|1,  0(default) => this ds is not master ds, 1 => this ds is master ds\n");
        printf("delete a file.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      int32_t unlink_type = 0;
      int32_t option_flag = 0;
      int32_t is_master = 0;
      if (param.size() > 2)
      {
        unlink_type = atoi(const_cast<char*> (param[2].c_str()));
      }
      if (param.size() > 3)
      {
        option_flag = atoi(const_cast<char*> (param[3].c_str()));
      }
      if (param.size() > 4)
      {
        is_master = atoi(const_cast<char*> (param[4].c_str()));
      }
      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_file_id;
      ds_task.unlink_type_ = unlink_type;
      ds_task.option_flag_ = option_flag;
      ds_task.is_master_ = is_master;
      ret = DsLib::unlink_file(ds_task);
      break;
    }
  case CMD_RENAME_FILE:
    {
      if (param.size() != 3)
      {
        printf("Usage:rename_file block_id old_file_id new_file_id\n");
        printf("rename file id.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_old_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_new_file_id = strtoul(const_cast<char*> (param[2].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task.block_id_ = ds_block_id;
      ds_task.old_file_id_ = ds_old_file_id;
      ds_task.new_file_id_ = ds_new_file_id;
      ret = DsLib::rename_file(ds_task);
      break;
    }
  case CMD_READ_FILE_INFO:
    {
      if (param.size() < 2 || param.size() > 3)
      {
        printf("Usage:read_file_info block_id file_id [ds_mode]\n");
        printf("      ds_mode  0|1,  0          => exclude deleted file\n\
                                     1(default) => include deleted file\n");
        printf("get the file information.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      int32_t ds_mode = 1;
      if (3 == param.size())
      {
        ds_mode = atoi(const_cast<char*> (param[2].c_str()));
      }
      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_file_id;
      ds_task.mode_ = ds_mode;
      ret = DsLib::read_file_info(ds_task);
      break;
    }
  case CMD_SEND_CRC_ERROR:
    {
      if (param.size() < 3)
      {
        printf("Usage: send_crc_error block_id file_id crc [error_flag] [ds_addr1 ds_addr2 ...]\n");
        printf("       error_flag  0|1|2,  0(default)          0 => crc partial error, 1 => crc all error, 2 => block eio error\n");
        printf("       ds_addr1   xx.xx.xx.xx:xxxx,       this_ds(default)\n");
        printf("label file crc error or block eio error, will repair file or block \n");
        break;
      }

      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint32_t ds_crc = strtoul(const_cast<char*> (param[2].c_str()), reinterpret_cast<char**> (NULL), 10);

      int32_t ds_error_flag = 0;
      if (param.size() > 3)
      {
        ds_error_flag = atoi(const_cast<char*> (param[3].c_str()));
      }

      int32_t ds_snum = 1;
      VUINT64 fail_servers;
      if (param.size() > 4)
      {
        ds_snum = param.size() - 4;
        int32_t i = 0;
        int32_t param_size = param.size();
        for (i = 4; i < param_size; i++)
        {

          fail_servers.push_back(Func::get_host_ip(param[i].c_str()));
        }
      }
      else
      {
        fail_servers.push_back(ds_ip);
      }

      ds_task.block_id_ = ds_block_id;
      ds_task.new_file_id_ = ds_file_id;
      ds_task.crc_ = ds_crc;
      ds_task.option_flag_ = ds_error_flag;
      ds_task.failed_servers_ = fail_servers;
      ret = DsLib::send_crc_error(ds_task);
      break;
    }
  case CMD_LIST_BITMAP:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_bitmap type\n");
        printf("      type  0|1,  0          => normal bitmap\n\
                                  1          => error bitmap\n");
        printf("list the bitmap of the server.\n");
        break;
      }
      int type = atoi(const_cast<char*> (param[0].c_str()));
      ds_task.list_block_type_ = type;
      ret = DsLib::list_bitmap(ds_task);
      break;
    }
  case CMD_UNKNOWN:
    fprintf(stderr, "unknown command.\n");
    ret = show_help(param);
    break;
  case CMD_HELP:
    ret = show_help(param);
    break;
  default:
    break;
  }

  return ret;
}

int main_loop()
{
  VSTRING param;
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = dscmd_completion;
#else
  char cmd_line[MAX_CMD_SIZE];
#endif

  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("DataServer> ");
    if (!cmd_line)
#else
    fprintf(stderr, "DataServer> ");
    if (NULL == fgets(cmd_line, MAX_CMD_SIZE, stdin))
#endif
    {
      continue;
    }

    int cmd = parse_cmd(cmd_line, param);
#ifdef _WITH_READ_LINE
    delete cmd_line;
    cmd_line = NULL;
#endif
    if (cmd == CMD_QUIT)
    {
      break;
    }
    switch_cmd(cmd, param);
  }
  return TFS_SUCCESS;
}

//show help tips
int show_help(VSTRING &)
{
  printf("COMMAND SET:\n"
    "get_server_status  nums                                                     get the information of the nums most frequently visited blocks in dataserver.\n"
    "get_ping_status                                                             get the ping status of dataServer.\n"
    "list_block  type                                                            list all the blocks in a dataserver.\n"
    "get_block_info  block_id                                                    get the information of a block in the dataserver.\n"
    "list_file  block_id                                                         list all the files in a block.\n"
    "read_file_data  blockid fileid local_file_name                              download a tfs file to local.\n"
    "write_file_data block_id file_id local_file_name                            upload a local file to tfs\n"
    "unlink_file  block_id file_id [unlink_type] [option_flag] [is_master]       delete a file.\n"
    "read_file_info  block_id file_id [ds_mode]                                  get the file information.\n"
    "list_bitmap  type                                                           list the bitmap of the server\n"
    "create_file_id block_id file_id                                             create a new file_id\n"
    "remove_block block_id                                                       remove a block\n"
    "reset_block_version block_id                                                 reset block version to 1, will not affect files in the block\n"
    "rename_file block_id old_file_id new_file_id                                rename old file_id to a new file_id\n"
    "send_crc_error  block_id file_id crc [error_flag] [ds_addr1 ds_addr2 ...]   label file crc error or block eio error, will repair file or block\n"
    "quit                                                                        quit console.\n"
    "help                                                                        show help.\n\n");
  return TFS_SUCCESS;
}
