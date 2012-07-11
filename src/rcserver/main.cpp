/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: main.cpp 495 2011-06-14 08:47:12Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "rc_service.h"

int main(int argc, char* argv[])
{
  tfs::rcserver::RcService service;
  return service.main(argc, argv);
}
