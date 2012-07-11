/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: adminserver.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#include "common/internal.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/admin_cmd_message.h"

#include "tools/util/tool_util.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

using namespace std;
typedef map<string, VSTRING> MSTR_VSTR;
typedef map<string, VSTRING>::iterator MSTR_VSTR_ITER;

static STR_FUNC_MAP g_cmd_map;
static MSTR_VSTR g_server_map;
static char* g_cur_cmd;
static const int32_t g_default_port = 12000;

static const char* SUC_COLOR = "\033[32m";
static const char* FAIL_COLOR = "\033[31m";

inline bool is_whitespace(char c);

#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

// just trick. complete base on previous word count, occasionally match sequence
enum
{
  CMD_COMPLETE = 0,
  SERVER_COMPLETE,
  INDEX_COMPLETE,
  STOP_COMPLETE
};

static char g_cur_server[MAX_CMD_SIZE+1];

// get nth word in current input to store, base on keyth word to check count
int get_nth_word(int32_t nth, char* store, int32_t keyth)
{
  int32_t count = 0, pos = 0, start = 0, key_start = 0, key_end = 0, end = rl_point;
  store[0] = '\0';

  // reach current word's start
  while (end && !is_whitespace(rl_line_buffer[end]))
  {
    end--;
  }

  while (pos < end)
  {
    // reach next word's start
    while (pos < end && is_whitespace(rl_line_buffer[pos]))
    {
      pos++;
    }

    start = pos;
    // reach next word's end
    while (pos < end && !is_whitespace(rl_line_buffer[pos]))
    {
      pos++;
    }

    if (pos != start)
    {
      count++;
    }

    if (keyth == count)         // found key
    {
      key_start = start;
      key_end = pos;
    }
    if (nth == count)           // found nth
    {
      int len = pos - start;
      strncpy(store, rl_line_buffer+start, len);
      store[len] = '\0';
    }
  }

  if (count >= keyth)                // already have key, check count
  {
    char* key = rl_copy_text(key_start, key_end);
    STR_FUNC_MAP_ITER it;
    if ((it = g_cmd_map.find(key)) != g_cmd_map.end()) // valid key
    {
      if (count > it->second.min_param_cnt_) // over count
      {
        if (count < it->second.max_param_cnt_)           // valid over count
        {
          count = INDEX_COMPLETE;
        }
        else
        {
          count = STOP_COMPLETE;
        }
      }
    }
    else                        // invalid key
    {
      count = STOP_COMPLETE;
    }
    free(key);
    key = NULL;
  }

  return count;
}

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

char* match_server(const char* text, int state)
{
  return do_match(text, state, g_server_map);
}

char* match_index(const char* text, int state)
{
  MSTR_VSTR_ITER it = g_server_map.find(g_cur_server);
  if (it != g_server_map.end())
  {
    return do_match(text, state, it->second);
  }
  else
  {
    return NULL;
  }
}

char** admin_cmd_completion (const char* text, int, int)
{
  // disable default filename completion
  rl_attempted_completion_over = 1;

  switch (get_nth_word(2, g_cur_server, 1)) // command is first filed, server is second
  {
  case CMD_COMPLETE:
    return rl_completion_matches(text, match_cmd);
  case SERVER_COMPLETE:
    return rl_completion_matches(text, match_server);
  case INDEX_COMPLETE:
    return rl_completion_matches(text, match_index);
  case STOP_COMPLETE:
  default:
    return (char**)NULL;
  }
}
#endif

int main_loop();
int do_cmd(char*);

int do_monitor(const VSTRING&, int32_t);

int cmd_show_help(const VSTRING&);
int cmd_quit(const VSTRING&);
int cmd_show_server(const VSTRING&);
int cmd_show_status(const VSTRING&);
int cmd_show_status_all(const VSTRING&);
int cmd_check(const VSTRING&);
int cmd_check_all(const VSTRING&);
int cmd_start_monitor(const VSTRING&);
int cmd_restart_monitor(const VSTRING&);
int cmd_stop_monitor(const VSTRING&);
int cmd_start_index(const VSTRING&);
int cmd_stop_index(const VSTRING&);
int cmd_kill_admin(const VSTRING&);

void init()
{
  g_cmd_map["help"] = CmdNode("help", "show help info", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit", "quit", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit", "exit", 0, 0, cmd_quit);
  g_cmd_map["show"] = CmdNode("show", "show server", 0, 0, cmd_show_server);
  g_cmd_map["status"] = CmdNode("status server", "show server status", 1, 1, cmd_show_status);
  g_cmd_map["astatus"] = CmdNode("astatus", "show all server status", 0, 0, cmd_show_status_all);
  g_cmd_map["check"] = CmdNode("check server", "check  server", 1, 1, cmd_check);
  g_cmd_map["acheck"] = CmdNode("acheck", "check all server", 0, 0, cmd_check_all);
  g_cmd_map["mstart"] = CmdNode("mstart server", "start server monitor", 1, 1, cmd_start_monitor);
  g_cmd_map["mrestart"] = CmdNode("mrestart server", "restart server monitor", 1, 1, cmd_restart_monitor);
  g_cmd_map["mstop"] = CmdNode("mstop server", "stop server monitor", 1, 1, cmd_stop_monitor);
  g_cmd_map["istart"] = CmdNode("istart server index", "start index in server", 2, INT_MAX, cmd_start_index);
  g_cmd_map["istop"] = CmdNode("istop server index", "stop index in server", 2, INT_MAX, cmd_stop_index);
  g_cmd_map["kill"] = CmdNode("kill server", "kill adminserver in server", 1, 1, cmd_kill_admin);
}

static void sign_handler(int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "adminserver tool exit.\n");
    exit(TFS_ERROR);
    break;
  }
}

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -f server_list_file [-n] [-i] [-h]\n", name);
  fprintf(stderr, "       -f server_list_file specify server list file\n");
  fprintf(stderr, "       -n set log level\n");
  fprintf(stderr, "       -i directly execute the command\n");
  fprintf(stderr, "       -h help\n");
}

inline int32_t get_port(char* addr)
{
  char *pos = strrchr(addr, ':');
  if (pos != NULL)
  {
    *pos++ = '\0';
    return atoi(pos);
  }
  return g_default_port;
}

inline bool is_whitespace(char c)
{
  return (' ' == c || '\t' == c);
}

inline char* strip_line(char* line)
{
  while (is_whitespace(*line))
  {
    line++;
  }
  int32_t end = strlen(line);
  while (end && (is_whitespace(line[end-1]) || '\n' == line[end-1] || '\r' == line[end-1]))
  {
    end--;
  }
  line[end] = '\0';
  return line;
}

int get_server_list(const char* conf_file)
{
  FILE* fp = fopen(conf_file, "r");
  if (!fp)
  {
    fprintf(stderr, "can't open server list file %s : %s\n", conf_file, strerror(errno));
    return TFS_ERROR;
  }

  char buf[MAX_CMD_SIZE+1];
  char* tmp = NULL;
  while (fgets(buf, MAX_CMD_SIZE, fp))
  {
    tmp = strip_line(buf);
    if (tmp[0]) // not blank line
    {
      g_server_map.insert(MSTR_VSTR::value_type(tmp, VSTRING()));
    }
  }
  return TFS_SUCCESS;
}

int main(int argc, char* argv[])
{
  int32_t i;
  bool directly = false;
  bool set_log_level = false;
  const char* conf_file = NULL;

  // analyze arguments
  while ((i = getopt(argc, argv, "f:nih")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'n':
      set_log_level = true;
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

  if (conf_file)
  {
    get_server_list(conf_file);
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }
  tfs::message::MessageFactory factory;
  tfs::common::BasePacketStreamer streamer;
  streamer.set_packet_factory(&factory);
  NewClientManager::get_instance().initialize(&factory, &streamer);

  init();

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else if (directly)
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

  return TFS_SUCCESS;
}

inline void print_head()
{
  fprintf(stderr, "\033[36m index    pid  restart   fail  dead_cnt    start_time          dead_time\033[0m\n");
  fprintf(stderr, " ------  -----  -----   ----   ------   ----------------       ------------------ \n");
}

int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = admin_cmd_completion;
#else
  char cmd_line[MAX_CMD_SIZE];
#endif
  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("admin> ");
    if (!cmd_line)
#else
      fprintf(stderr, "admin> ");
    if (NULL == fgets(cmd_line, MAX_CMD_SIZE, stdin))
#endif
    {
      continue;
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
  // ok this is current command
  g_cur_cmd = key;

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

int do_monitor(const VSTRING& param, int32_t type)
{
  int ret = TFS_SUCCESS;
  string server = param[0];

  uint64_t ip_port = 0;
  {
    char ip[MAX_CMD_SIZE+1];     // to support mutiple port in the same server, must copy
    ip[MAX_CMD_SIZE] = '\0';
    int32_t port = get_port(strncpy(ip, server.c_str(), MAX_CMD_SIZE));
    ip_port = Func::str_to_addr(ip, port);
  }

  AdminCmdMessage admin_msg(type);
  for (size_t i = 1; i < param.size(); i++)
  {
    admin_msg.set_index(param[i]);
  }

  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* message = NULL;
    if (TFS_SUCCESS == (ret = send_msg_to_server(ip_port, client, &admin_msg, message)))
    {
      if (STATUS_MESSAGE == message->getPCode())
      {
        StatusMessage* msg = dynamic_cast<StatusMessage*>(message);
        ret = msg->get_status();
        fprintf(stderr, "%s %s \033[0m\n", ret != TFS_SUCCESS ? FAIL_COLOR : SUC_COLOR, msg->get_error());
      }
      else if (ADMIN_CMD_MESSAGE == message->getPCode() &&
          ADMIN_CMD_RESP == dynamic_cast<AdminCmdMessage*>(message)->get_cmd_type())
      {
        vector<MonitorStatus>* m_status =  dynamic_cast<AdminCmdMessage*>(message)->get_status();
        int32_t size = m_status->size();
        fprintf(stderr, "\033[36m       ================== %s count: %d ===================\033[0m\n",
            server.c_str(), size);

        // update server ==> index
        VSTRING& index = g_server_map[server];
        index.clear();

        for (int32_t i = 0; i < size; i++)
        {
          (*m_status)[i].dump();
          index.push_back((*m_status)[i].index_);
        }
        fprintf(stderr, "\033[36m-----------------------------------------------------------------------------------\033[0m\n\n");
      }
      else
      {
        fprintf(stderr, "%s unknown message recieved, op cmd fail\n \033[0m", FAIL_COLOR);
      }
    }
    else
    {
      fprintf(stderr, "send message error");
    }
    NewClientManager::get_instance().destroy_client(client);

  }
  else
  {
    fprintf(stderr, "create client error");
    ret = TFS_ERROR;
  }

  return ret;
}

int cmd_show_help(const VSTRING&)
{
  fprintf(stderr, "\nsupported command:");
  for (STR_FUNC_MAP_ITER it = g_cmd_map.begin(); it != g_cmd_map.end(); it++)
  {
    fprintf(stderr, "\n%-40s %s", it->second.syntax_, it->second.info_);
  }
  fprintf(stderr, "\n\n");
  return TFS_SUCCESS;
}

int cmd_quit(const VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_show_server(const VSTRING&)
{
  int32_t i = 1;
  MSTR_VSTR::iterator it;
  for (it = g_server_map.begin(); it != g_server_map.end(); it++)
  {
    fprintf(stderr, "== %d ==  %s\n", i++, it->first.c_str());
  }
  return TFS_SUCCESS;
}

int cmd_show_status(const VSTRING& param)
{
  print_head();
  return do_monitor(param, ADMIN_CMD_GET_STATUS);
}

int cmd_show_status_all(const VSTRING&)
{
  print_head();
  // for uniformity, a little time waste
  MSTR_VSTR::iterator it;
  VSTRING param;
  for (it = g_server_map.begin(); it != g_server_map.end(); it++)
  {
    param.clear();
    param.push_back(it->first);
    do_monitor(param, ADMIN_CMD_GET_STATUS);
  }
  return TFS_SUCCESS;
}

int cmd_check(const VSTRING& param)
{
  print_head();
  return do_monitor(param, ADMIN_CMD_CHECK);
}

int cmd_check_all(const VSTRING&)
{
  print_head();
  // for uniformity, a little time waste
  MSTR_VSTR::iterator it;
  VSTRING param;
  for (it = g_server_map.begin(); it != g_server_map.end(); it++)
  {
    param.clear();
    param.push_back(it->first);
    do_monitor(param, ADMIN_CMD_CHECK);
  }
  return TFS_SUCCESS;
}

int cmd_start_monitor(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_START_MONITOR);
}

int cmd_restart_monitor(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_RESTART_MONITOR);
}

int cmd_stop_monitor(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_STOP_MONITOR);
}

int cmd_start_index(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_START_INDEX);
}

int cmd_stop_index(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_STOP_INDEX);
}

int cmd_kill_admin(const VSTRING& param)
{
  return do_monitor(param, ADMIN_CMD_KILL_ADMINSERVER);
}
