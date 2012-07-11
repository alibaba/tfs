/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ds_lib.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */
#ifndef TFS_TOOLS_UTIL_TOOL_UTIL_H_
#define TFS_TOOLS_UTIL_TOOL_UTIL_H_

#include "common/internal.h"

namespace tfs
{
  namespace tools
  {

    typedef int (*cmd_function)(const common::VSTRING&);
    struct CmdNode
    {
      const char* syntax_;
      const char* info_;
      int32_t min_param_cnt_;
      int32_t max_param_cnt_;
      cmd_function func_;

      CmdNode()
      {
      }

      CmdNode(const char* syntax, const char* info, const int32_t min_param_cnt, const int32_t max_param_cnt, const cmd_function func) :
        syntax_(syntax), info_(info), min_param_cnt_(min_param_cnt), max_param_cnt_(max_param_cnt), func_(func)
      {
      }
    };


    typedef std::map<std::string, CmdNode> STR_FUNC_MAP;
    typedef STR_FUNC_MAP::iterator STR_FUNC_MAP_ITER;
    typedef STR_FUNC_MAP::const_iterator STR_FUNC_MAP_CONST_ITER;

    static const int TFS_CLIENT_QUIT = 0xfff1234;

    class ToolUtil
    {
      ToolUtil();
      ~ToolUtil();

    public:
      static int get_block_ds_list(const uint64_t server_id, const uint32_t block_id,
                                   common::VUINT64& ds_list, const int32_t flag = common::T_READ);
      static int show_help(const STR_FUNC_MAP& cmd_map);
      static void print_info(const int status, const char* fmt, ...);
    };
  }
}

#endif
