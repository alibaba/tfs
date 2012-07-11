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

#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/block_info_message.h"

#include "tool_util.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace tools
  {
    int ToolUtil::get_block_ds_list(const uint64_t server_id, const uint32_t block_id, VUINT64& ds_list, const int32_t flag)
    {
      int ret = TFS_ERROR;
      if (0 == server_id)
      {
        TBSYS_LOG(ERROR, "server is is invalid: %"PRI64_PREFIX"u", server_id);
      }
      else
      {
        GetBlockInfoMessage gbi_message(flag);
        gbi_message.set_block_id(block_id);

        tbnet::Packet* rsp = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(server_id, client, &gbi_message, rsp);

        if (rsp != NULL)
        {
          if (rsp->getPCode() == SET_BLOCK_INFO_MESSAGE)
          {
            ds_list = dynamic_cast<SetBlockInfoMessage*>(rsp)->get_block_ds();
          }
          else if (rsp->getPCode() == STATUS_MESSAGE)
          {
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
            fprintf(stderr, "get block info fail, error: %s\n,", dynamic_cast<StatusMessage*>(rsp)->get_error());
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
          }
        }
        else
        {
          fprintf(stderr, "get NULL response message, ret: %d\n", ret);
        }

        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

    int ToolUtil::show_help(const STR_FUNC_MAP& cmd_map)
    {
      fprintf(stdout, "\nsupported command:");
      for (STR_FUNC_MAP_CONST_ITER it = cmd_map.begin(); it != cmd_map.end(); it++)
      {
        fprintf(stdout, "\n%-40s %s", it->second.syntax_, it->second.info_);
      }
      fprintf(stdout, "\n\n");

      return TFS_SUCCESS;
    }

    void ToolUtil::print_info(const int status, const char* fmt, ...)
    {
      va_list args;
      va_start(args, fmt);
      FILE* out = NULL;
      const char* str = NULL;

      if (TFS_SUCCESS == status)
      {
        out = stdout;
        str = "success";
      }
      else
      {
        out = stderr;
        str = "fail";
      }
      fprintf(out, fmt, va_arg(args, const char*));
      fprintf(out, " %s.\n", str);
      va_end(args);
    }

  }
}
