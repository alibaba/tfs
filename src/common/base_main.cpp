/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *
 */


#include <tbsys.h>

#include "func.h"
#include "define.h"
#include "base_main.h"
#include "error_msg.h"
#include "config_item.h"
#include "directory_op.h"

namespace tfs
{
  namespace common
  {
#if defined(__DATE__) && defined(__TIME__) && defined(PACKAGE) && defined(VERSION)
static const char g_build_description[] = "Taobao file system(TFS), Version: " VERSION ", Build time: "__DATE__ " "__TIME__;
#else
static const char _g_build_description[] = "unknown";
#endif
    BaseMain* BaseMain::instance_=NULL;
    static void ctrlc_handler_callback( int sig )
    {
      BaseMain* service = BaseMain::instance();
      assert( service != 0 );
      service->handle_interrupt( sig );
    }

    BaseMain::BaseMain():
      stop_(false)
    {
      assert(instance_ == NULL );
      instance_ = this;
    }

    BaseMain::~BaseMain()
    {
      instance_ = NULL;
    }

    int BaseMain::main(int argc,char*argv[])
    {
      bool daemonize(false);
      int32_t idx = 1;
      int32_t iret = argc < 2 ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != iret)
      {
        std::cerr<<":invalid option\n"<<"Try `--help' for more information"<<std::endl;
      }
      else
      {
        while(idx < argc )
        {
          if(strcmp(argv[idx],"-h") == 0 || strcmp(argv[idx],"--help") == 0)
          {
            help();//show help
            iret = TFS_ERROR;
            break;
          }
          else if (strcmp(argv[idx], "-v" ) == 0 || strcmp(argv[idx], "--version" ) == 0)
          {
            version();//show version
            iret = TFS_ERROR;
            break;
          }
          else if(strcmp(argv[idx], "-d")== 0)
          {
            for(int i = idx; i + 1 < argc; ++i)
            {
              argv[i] = argv[i + 1];
            }
            argc -= 1;
            daemonize = true;
          }
          else if(strcmp(argv[idx],"-f" ) == 0)
          {
            if(idx + 1 < argc)
            {
              config_file_=argv[idx + 1];
            }
            else
            {
              std::cerr<<"-f must be followed by an argument...."<<std::endl;
              iret = TFS_ERROR;
              break;
            }
            for(int i = idx ; i + 2 < argc; ++i)
            {
              argv[i] = argv[i + 2];
            }
            argc -= 2;
            if (config_file_.empty())
            {
              std::cerr<<"-f must be followed an argument,argument is not null"<<std::endl;
              iret = TFS_ERROR;
              break;
            }
          }
          else
          {
            ++idx;
          }//end if
        }//end while idx<argc

        if (TFS_SUCCESS == iret)
        {
          std::string errmsg;
          iret = parse_common_line_args(argc, argv, errmsg);
          if (TFS_SUCCESS != iret)
          {
            std::cerr << "parse common line args error: " << errmsg << std::endl;
          }
          else
          {
            iret = start(argc , argv, daemonize);
          }
        }
      }
      return iret;
    }

    BaseMain* BaseMain::instance()
    {
      return instance_;
    }

    int BaseMain::start(int argc , char* argv[], const bool daemon)
    {
      // load config file
      int32_t iret = TBSYS_CONFIG.load(config_file_.c_str());
      if (EXIT_SUCCESS != iret)
      {
        std::cerr << "load config error config file is " << config_file_ << std::endl;
        iret = TFS_ERROR;
      }

      //initilaize work dir
      if (TFS_SUCCESS == iret)
      {
        iret = initialize_work_dir(argv[0]);
      }

      //initialize pid file
      if (TFS_SUCCESS == iret)
      {
        if (daemon)
        {
          iret = initialize_pid_file(argv[0]);
        }
      }

      //initialize log file
      if (TFS_SUCCESS == iret)
      {
        iret = initialize_log_file(argv[0]);
      }

      if (TFS_SUCCESS == iret)
      {
        signal(SIGPIPE, SIG_IGN);
        int32_t pid = 0;
        if (daemon)
        {
          //pid = tbsys::CProcess::startDaemon(pid_file_path_.c_str(), log_file_path_.c_str());
          pid = Func::start_daemon(pid_file_path_.c_str(), log_file_path_.c_str());
        }

        if (0 == pid)//child process
        {
          signal(SIGHUP, ctrlc_handler_callback);
          signal(SIGTERM, ctrlc_handler_callback);
          signal(SIGINT, ctrlc_handler_callback);
          signal(40, ctrlc_handler_callback);
          signal(41, ctrlc_handler_callback);
          signal(42, ctrlc_handler_callback);

          iret = run(argc, argv);
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "%s initialze failed, exit", argv[0]);
          }
          if (TFS_SUCCESS == iret)
          {
            TBSYS_LOG(INFO, "%s initialize successful, wait for shutdown", argv[0]);
            wait_for_shutdown();
          }
          destroy();
          TBSYS_LOG(INFO, "%s destroyed successful", argv[0]);
        }
      }
      return iret;
    }

    void BaseMain::stop()
    {
      shutdown();
    }

    int BaseMain::shutdown()
    {
      TBSYS_LOG(DEBUG, "notifyAll");
      tbutil::Monitor<tbutil::Mutex>::Lock sync(monitor_);
      if ( !stop_ )
      {
        stop_= true;
        monitor_.notifyAll();
        TBSYS_LOG(DEBUG, "notifyAll");
      }
      return TFS_SUCCESS;
    }

    int BaseMain::wait_for_shutdown()
    {
      TBSYS_LOG(DEBUG, "wait for shutdown");
      tbutil::Monitor<tbutil::Mutex>::Lock sync(monitor_);
      while( !stop_ )
      {
        monitor_.wait();
      }
      TBSYS_LOG(DEBUG, "wait for shutdown");
      return TFS_SUCCESS;
    }

    int BaseMain::handle_interrupt(int32_t sig)
    {
      TBSYS_LOG(INFO, "receive sig: %d", sig);
      switch (sig)
      {
        case SIGHUP:
          break;
        case SIGTERM:
        case SIGINT:
          stop();
          break;
        case 40:
          TBSYS_LOGGER.checkFile();
          break;
        case 41:
        case 42:
          if(sig == 41)
          {
            TBSYS_LOGGER._level++;
          }
          else
          {
            TBSYS_LOGGER._level--;
          }
          TBSYS_LOG(INFO, "TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
          break;
        default:
          break;
      }
      return TFS_SUCCESS;
    }

    void BaseMain::help()
    {
      std::string options=
        "Options:\n"
        "-h,--help          show this message...\n"
        "-v,--version       show porgram version...\n"
        "-d                 run as a daemon...\n"
        "-f file            configure files...\n";
        std::cerr << "Usage:\n" << options;
    }

    void BaseMain::version()
    {
      std::cerr << g_build_description << std::endl;
    }

    int BaseMain::parse_common_line_args(int , char* [], std::string& )
    {
      return TFS_SUCCESS;
    }

    /** get work directory*/
    const char* BaseMain::get_work_dir() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_WORK_DIR, "/tmp");
    }

    const char* BaseMain::get_log_file_level() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_LOG_LEVEL, "debug");
    }

    const char* BaseMain::get_log_path() const
    {
      return log_file_path_.empty() ? NULL : log_file_path_.c_str();
    }

    int64_t BaseMain::get_log_file_size() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_SIZE, 0x40000000);
    }

    int32_t BaseMain::get_log_file_count() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_NUM, 16);
    }

    int BaseMain::initialize_work_dir(const char* app_name)
    {
      int32_t iret = TFS_SUCCESS;
      const char* work_dir = get_work_dir();
      if (NULL == work_dir)
      {
        std::cerr << app_name << " not set workdir" <<std::endl;
        iret = EXIT_CONFIG_ERROR;
      }

      if (TFS_SUCCESS == iret)
      {
        if (!DirectoryOp::create_full_path(work_dir))
        {
          std::cerr << app_name << " create workdir" << work_dir <<"error: "<< strerror(errno) << std::endl;
          iret = EXIT_MAKEDIR_ERROR;
        }
      }
      return iret;
    }

    int BaseMain::initialize_log_file(const char* app_name)
    {
      const char* work_dir = get_work_dir();
      int32_t iret =  NULL == work_dir ? EXIT_CONFIG_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS != iret)
      {
        std::cerr << app_name << " not set workdir" <<std::endl;
      }
      if (TFS_SUCCESS == iret)
      {
        const char* const tmp_path = get_log_file_path();
        std::string log_path(NULL == tmp_path ? "" : tmp_path);
        if (log_path.empty()
            || log_path == "")
        {
          log_path = work_dir;
          log_path += "/logs/";
          std::string tmp(app_name);
          std::string::size_type pos = tmp.find_last_of('/');
          std::string name = tmp.substr(pos);
          if (!name.empty() && name.c_str()[0] == '/')
          {
            name = tmp.substr(pos + 1);
          }
          log_path += std::string::npos == pos || name.empty() ? "base_service" : name;
          log_path += ".log";
        }
        log_file_path_ = log_path;

        if (0 == access(log_path.c_str(), R_OK))
        {
          TBSYS_LOGGER.rotateLog(log_path.c_str());
        }
        else
        {
          if (!DirectoryOp::create_full_path(log_path.c_str(), true))
          {
            std::cerr << app_name << "create log directory" << log_path << "error: " << strerror(errno) << std::endl;
            iret = EXIT_MAKEDIR_ERROR;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOGGER.setLogLevel(get_log_file_level());
          TBSYS_LOGGER.setMaxFileSize(get_log_file_size());
          TBSYS_LOGGER.setMaxFileIndex(get_log_file_count());
        }
      }
      return iret;
    }

    int BaseMain::initialize_pid_file(const char* app_name)
    {
      const char* work_dir = get_work_dir();
      int32_t iret =  NULL == work_dir ? EXIT_CONFIG_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS != iret)
      {
        std::cerr << app_name << " not set workdir" <<std::endl;
      }
      if (TFS_SUCCESS == iret)
      {
        const char* const tmp_path = get_pid_file_path();
        std::string pid_path(NULL == tmp_path ? "" : tmp_path);
        if (pid_path.empty()
            || pid_path == "")
        {
          pid_path = work_dir;
          pid_path += "/logs/";
          std::string tmp(app_name);
          std::string::size_type pos = tmp.find_last_of('/');
          std::string name = tmp.substr(pos);
          if (!name.empty() && name.c_str()[0] == '/')
          {
            name = tmp.substr(pos + 1);
          }
          pid_path += std::string::npos == pos || name.empty() ? "base_main" : name;
          pid_path += ".pid";
        }
        pid_file_path_ = pid_path;

        if (0 != access(pid_path.c_str(), R_OK))
        {
          if (!DirectoryOp::create_full_path(pid_path.c_str(), true))
          {
            std::cerr << app_name << " create pid directory" << pid_path << "error: " << strerror(errno) << std::endl;
            iret = EXIT_MAKEDIR_ERROR;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          int32_t pid = 0;
          if ((pid = tbsys::CProcess::existPid(pid_file_path_.c_str())))
          {
            std::cerr << app_name << " has been exist: pid: " << pid << std::endl;
            iret = TFS_ERROR;
          }
        }
      }
      return iret;
    }
  }/** common **/
}/** tfs **/

