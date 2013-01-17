/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <sys/resource.h>

#include "func.h"
#include "error_msg.h"
#include <tbsys.h>
using namespace std;

namespace tfs
{
  namespace common
  {
    // disk usage
    int Func::get_disk_usage(const char* path, int64_t* used_bytes, int64_t* total_bytes)
    {
      *used_bytes = 1;
      *total_bytes = 1;
      struct statfs buf;
      if (statfs(path, &buf) != 0)
      {
        return EXIT_FILESYSTEM_ERROR;
      }
      *used_bytes = (int64_t)(buf.f_blocks - buf.f_bfree) * buf.f_bsize;
      *total_bytes = (int64_t)(buf.f_blocks) * buf.f_bsize;
      return TFS_SUCCESS;
    }

    // get avg load in one minute
    int Func::get_load_avg()
    {
      int l = 0;
      double d;
      if (getloadavg(&d, 1) == 1)
      {
        l = (int) (d * 100);
        if (l < 10)
          l = 10;
      }
      return l;
    }

    // create dir
    int Func::make_directory(char* dirpath)
    {
      struct stat stats;
      if (lstat(dirpath, &stats) == 0 && S_ISDIR(stats.st_mode))
        return (TFS_SUCCESS);

      mode_t umask_value = umask(0);
      umask(umask_value);
      mode_t mode = ((S_IRWXU | S_IRWXG | S_IRWXO) & (~umask_value)) | S_IWUSR | S_IXUSR;

      char* slash = dirpath;
      while (*slash == '/')
        slash++;

      while (1)
      {
        slash = strchr(slash, '/');
        if (slash == NULL)
          break;

        *slash = '\0';
        mkdir(dirpath, mode);
        *slash++ = '/';
        while (*slash == '/')
          slash++;
      }
      if (mkdir(dirpath, mode) == -1)
        return EXIT_FILESYSTEM_ERROR;
      else
        return TFS_SUCCESS;
    }

    int Func::get_base_name(char* path, char* dirpath)
    {
      int length = strlen(path);
      char* slash = path + length - 1;
      while (*slash == '/')
      {
        slash--;
        length--;
      }
      for (; length > 0; length--)
      {
        if (path[length - 1] == '/')
        {
          length--;
          break;
        }
      }
      if (length == 0)
      {
        strcpy(dirpath, ".");
      }
      else
      {
        memcpy(dirpath, path, length);
        dirpath[length] = '\0';
      }
      return TFS_SUCCESS;
    }

		int Func::get_parent_dir(const char* file_path, char* dir_path, const int32_t dir_buf_len)
		{
			int ret = TFS_SUCCESS;
			if (NULL == file_path || NULL == dir_path || dir_buf_len <= 1) // at least .
			{
				ret = EXIT_PARAMETER_ERROR;
			}
			else
			{
 				int length = strlen(file_path);
				while (length > 0 && '/' == file_path[length-1])  // trailing slash
				{
					length--;
				}

      	for (; length > 0; length--)        // find slash
      	{
        	if ('/' ==file_path[length - 1])
        	{
          	break;
        	}
      	}


      	if (0 == length)
      	{
					strncpy(dir_path, ".", dir_buf_len);
      	}
     	  else
      	{
					if (dir_buf_len <= length)
					{
						ret = EXIT_PARAMETER_ERROR;
					}
					else
					{
        		strncpy(dir_path, file_path, length);
            dir_path[length] = '\0';
					}
     		}
			}
			return ret;
		}

    // get self ip
    uint32_t Func::get_local_addr(const char* dev_name)
    {
      int fd, intrface;
      struct ifreq buf[16];
      struct ifconf ifc;

      if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) <= 0)
      {
        return TFS_SUCCESS;
      }

      ifc.ifc_len = sizeof(buf);
      ifc.ifc_buf = (caddr_t) buf;
      if (ioctl(fd, SIOCGIFCONF, (char *) &ifc))
      {
        close(fd);
        return TFS_SUCCESS;
      }

      intrface = ifc.ifc_len / sizeof(struct ifreq);
      while (intrface-- > 0)
      {
        if (ioctl(fd, SIOCGIFFLAGS, (char *) &buf[intrface]))
        {
          continue;
        }
        if (buf[intrface].ifr_flags & IFF_LOOPBACK)
          continue;
        if (!(buf[intrface].ifr_flags & IFF_UP))
          continue;
        if (dev_name != NULL && strcmp(dev_name, buf[intrface].ifr_name))
          continue;
        if (!(ioctl(fd, SIOCGIFADDR, (char *) &buf[intrface])))
        {
          close(fd);
          return ((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr.s_addr;
        }
      }
      close(fd);
      return TFS_SUCCESS;
    }

    // ip is local addr
    bool Func::is_local_addr(const uint32_t ip)
    {
      int32_t intrface = 0;
      struct ifreq buf[16];
      struct ifconf ifc;
      bool bret = false;

      int32_t fd = socket(AF_INET, SOCK_DGRAM, 0);
      if (fd > 0)
      {
        ifc.ifc_len = sizeof(buf);
        ifc.ifc_buf = (caddr_t) buf;
        if (0 == ioctl(fd, SIOCGIFCONF, (char *) &ifc))
        {
          intrface = ifc.ifc_len / sizeof(struct ifreq);
          while (intrface-- > 0)
          {
            if (ioctl(fd, SIOCGIFFLAGS, (char *) &buf[intrface]))
            {
              continue;
            }
            if (buf[intrface].ifr_flags & IFF_LOOPBACK)
              continue;
            if (!(buf[intrface].ifr_flags & IFF_UP))
              continue;
            if (ioctl(fd, SIOCGIFADDR, (char *) &buf[intrface]))
            {
              continue;
            }
            if (((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr.s_addr == ip)
            {
              bret = true;
              break;
            }
          }
        }
        close(fd);
      }
      return bret;
    }

    // set local ip
    int Func::assign_ip_addr(const char* ifname, const char* ip, const char* mask)
    {
      int fd;
      struct ifreq ifr;
      struct sockaddr_in *addr = (struct sockaddr_in *) &ifr.ifr_addr;
      if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
      {
        return EXIT_GENERAL_ERROR;
      }
      memset(&ifr, 0, sizeof(struct ifreq));
      strncpy(ifr.ifr_name, ifname, sizeof(ifr.ifr_name));
      addr->sin_family = AF_INET;
      addr->sin_addr.s_addr = get_addr(ip);
      addr->sin_port = 0;
      if (ioctl(fd, SIOCSIFADDR, &ifr) < 0)
      {
        close(fd);
        TBSYS_LOG(ERROR, "SIOCSIFADDR: %s", strerror(errno));
        return EXIT_IOCTL_ERROR;
      }
      if (ioctl(fd, SIOCGIFFLAGS, &ifr) < 0)
      {
        close(fd);
        TBSYS_LOG(ERROR, "SIOCGIFFLAGS: %s", strerror(errno));
        return EXIT_IOCTL_ERROR;
      }
      ifr.ifr_flags |= IFF_UP | IFF_BROADCAST | IFF_RUNNING | IFF_MULTICAST;
      if (ioctl(fd, SIOCSIFFLAGS, &ifr) < 0)
      {
        close(fd);
        TBSYS_LOG(ERROR, "SIOCSIFFLAGS: %s", strerror(errno));
        return EXIT_IOCTL_ERROR;
      }
      if (mask != NULL)
      {
        addr = (struct sockaddr_in *) &ifr.ifr_netmask;
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = get_addr(mask);
        addr->sin_port = 0;
        if (ioctl(fd, SIOCSIFNETMASK, &ifr) < 0)
        {
          close(fd);
          TBSYS_LOG(ERROR, "SIOCSIFNETMASK: %s", strerror(errno));
          return EXIT_IOCTL_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    // convert ip to int
    // eg: 10.0.100.89 => 1499725834
    uint32_t Func::get_addr(const char* ip)
    {
      if (ip == NULL)
        return TFS_SUCCESS;
      int x = inet_addr(ip);
      if (x == (int) INADDR_NONE)
      {
        struct hostent *hp;
        if ((hp = gethostbyname(ip)) == NULL)
        {
          return TFS_SUCCESS;
        }
        x = ((struct in_addr *) hp->h_addr)->s_addr;
      }
      return x;
    }

    // convert uint64_t to string
#ifdef _XXXX___
    char* Func::addrToString(uint64_t ipport, char* str)
    {
      ip_addr* adr = (ip_addr *)(&ipport);
      unsigned char* bytes = (unsigned char *) &(adr->ip);
      if (adr->port > 0)
      {
        sprintf(str, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], adr->port);
      }
      else
      {
        sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
      }
      return str;
    }
#endif

    uint32_t Func::get_lan(const uint64_t ipport, const uint32_t ipmask)
    {
      IpAddr* adr = (IpAddr *) (&ipport);
      return (adr->ip_ & ipmask);
    }

    // convert ip, port to uint64_t
    uint64_t Func::str_to_addr(const char* ip, const int32_t port)
    {
      uint64_t ipport;
      IpAddr* adr = (IpAddr *) (&ipport);
      adr->ip_ = get_addr(ip);
      adr->port_ = port;
      return ipport;
    }
    std::string Func::addr_to_str(const uint64_t ipport, bool with_port)
    {
      char str[32];
      uint32_t ip = (uint32_t)(ipport & 0xffffffff);
      int port = (int)((ipport >> 32 ) & 0xffff);
      unsigned char *bytes = (unsigned char *) &ip;
      if (port > 0 && with_port) {
        sprintf(str, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
      } else {
        sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
      }
      return str;
    }

    //  port + inc
    /*uint64_t Func::addr_inc_port(const uint64_t ipport, const int32_t inc)
    {
      IpAddr* adr = (IpAddr *) (&ipport);
      adr->port_ += inc;
      return ipport;
    }*/

    uint64_t Func::get_host_ip(const char *s)
    {
      char addr[100];
      strncpy(addr, s, 100);
      addr[99] = '\0';
      char* pos = strchr(addr, ':');

      if (pos)
      {
          *pos++ = '\0';
      }
      else
      {
        return 0;
      }

      return Func::str_to_addr(addr, atoi(pos));
    }

    // convert to lower
    char* Func::str_to_lower(char* psz_buf)
    {
      if (psz_buf == NULL)
        return psz_buf;

      char* p = psz_buf;
      while (*p)
      {
        if ((*p) & 0x80)
          p++;
        else if ((*p) >= 'A' && (*p) <= 'Z')
          (*p) += 32;
        p++;
      }
      return psz_buf;
    }

    // convert to upper
    char* Func::str_to_upper(char* psz_buf)
    {
      if (psz_buf == NULL)
        return psz_buf;

      char* p = psz_buf;
      while (*p)
      {
        if ((*p) & 0x80)
          p++;
        else if ((*p) >= 'a' && (*p) <= 'z')
          (*p) -= 32;
        p++;
      }
      return psz_buf;
    }

    uint32_t Func::crc(uint32_t crc, const char* data, const int32_t len)
    {
      int32_t i;
      for (i = 0; i < len; ++i)
      {
        crc = (crc >> 8) ^ _crc32tab[(crc ^ (*data)) & 0xff];
        data++;
      }
      return (crc);
    }

    string Func::format_size(const int64_t c)
    {
      char s[128];
      double x = c;
      int level = 0;
      while (x >= 1000.0)
      {
        x /= 1024.0;
        level++;
        if (level >= 5)
          break;
      }
      if (level > 2)
      {
        snprintf(s, 128, "%.2f%c", x, _sizeunits[level - 1]);
      }
      else if (level > 0)
      {
        snprintf(s, 128, "%.1f%c", x, _sizeunits[level - 1]);
      }
      else
      {
        snprintf      (s, 128, "%" PRI64_PREFIX "d", c);
      }
      return s;
    }

    int64_t Func::curr_time()
    {
      struct timeval t;
      gettimeofday(&t, NULL);
      return (t.tv_sec * 1000000LL + t.tv_usec);
    }

    string Func::time_to_str(time_t t, int f)
    {
      char s[128];
      struct tm *tm = localtime((const time_t*) &t);
      if (f == 1)
      {
        snprintf(s, 128, "%04d%02d%02d%02d%02d%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour,
                 tm->tm_min, tm->tm_sec);
      }
      else
      {
        snprintf(s, 128, "%04d-%02d-%02d %02d:%02d:%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour,
                 tm->tm_min, tm->tm_sec);
      }
      return s;
    }

    char* Func::safe_malloc(const int32_t len, char* data)
    {
      if (len <= 0 || len > TFS_MALLOC_MAX_SIZE)
      {
        TBSYS_LOG(ERROR, "allocate to large memory: len: %d > maxlen: %"PRI64_PREFIX"d", len, TFS_MALLOC_MAX_SIZE);
        return NULL;
      }
      if (data == NULL)
      {
        return (char*) malloc(len);
      }
      else
      {
        return (char*) realloc(data, len);
      }
    }

    // sleep
    void Func::sleep(const float f_heart_interval, bool& stop)
    {
      int32_t heart_interval = static_cast<int32_t>(((f_heart_interval + 0.01) * 10));
      while (!stop && heart_interval > 0)
      {
        usleep(100000);
        heart_interval--;
      }
    }

    void Func::sleep(const float f_heart_interval, int32_t& stop)
    {
      int32_t heart_interval = static_cast<int32_t>(((f_heart_interval + 0.01) * 10));
      while (!stop && heart_interval > 0)
      {
        usleep(100000);
        heart_interval--;
      }
    }

    char* Func::subright(char* dst, char* src, int32_t n)
    {
      if (dst == NULL || src == NULL)
      {
        return NULL;
      }

      char* p = src;
      char* q = dst;
      int len = strlen(src);
      if (n > len)
        n = len;
      p += (len - n);
      while ((*q++ = *p++) != 0)
        ;

      return dst;
    }

    int Func::check_pid(const char* lock_file)
    {
      char buffer[64], *p;
      int otherpid = 0, lfp;
      lfp = open(lock_file, O_RDONLY, 0);
      if (lfp > 0)
      {
        read(lfp, buffer, 64);
        close(lfp);
        buffer[63] = '\0';
        p = strchr(buffer, '\n');
        if (p != NULL)
          *p = '\0';
        otherpid = atoi(buffer);
      }
      if (otherpid > 0)
      {
        if (kill(otherpid, 0) != 0)
        {
          otherpid = 0;
        }
      }
      return otherpid;
    }

    int Func::write_pid(const char* lock_file)
    {
      int lfp = open(lock_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
      if (lfp < 0)
        exit(1);
      if (lockf(lfp, F_TLOCK, 0) < 0)
      {
        fprintf(stderr, "already run.\n");
        exit(0);
      }

      char str[32];
      int pid = getpid();
      sprintf(str, "%d\n", getpid());
      write(lfp, str, strlen(str));
      close(lfp);
      return pid;
    }

    bool Func::hour_range(int min, int max)
    {
      time_t t = time(NULL);
      struct tm *tm = localtime((const time_t*) &t);
      bool reverse = false;
      if (min > max)
      {
        std::swap(min, max);
        reverse = true;
      }
      bool inrange = tm->tm_hour >= min && tm->tm_hour <= max;
      return reverse ? !inrange : inrange;
    }

    int32_t Func::split_string(const char* line, const char del, std::vector<std::string>& fields)
    {
      const char* start = line;
      const char* p = NULL;
      char buffer[BUFSIZ];
      while (start != NULL)
      {
        p = strchr(start, del);
        if (p != NULL)
        {
          //memset(buffer, 0, BUFSIZ);
          assert(p - start < BUFSIZ);
          strncpy(buffer, start, p - start);
          buffer[p - start] = 0;
          if (buffer[0] != '\0')
            fields.push_back(buffer);
          start = p + 1;
        }
        else
        {
          if (start[0] != '\0')
            fields.push_back(start);
          break;
        }
      }
      return fields.size();
    }

    // just modify tbsys::CProcess::startDaemon
    // close all opened file descriptor inherited
    int Func::start_daemon(const char *pid_file, const char *log_file)
    {
      if (getppid() == 1)
      {
        return 0;
      }

      int pid = fork();
      if (pid < 0)
      {
        exit(1);
      }

      if (pid > 0)
      {
        return pid;
      }

      tbsys::CProcess::writePidFile(pid_file);

      // close all opened file descriptor inherited from parent
      struct rlimit rl;
      int ret = TFS_SUCCESS;
      if ((ret = getrlimit(RLIMIT_NOFILE, &rl)) < 0)
      {
        fprintf(stderr, "can not get open file limit, ret %d : %s", ret, strerror(errno));
        return ret;
      }
      // maybe get open fd from /proc/xxx/fd ?
      int32_t file_limit = (RLIM_INFINITY == rl.rlim_max) ? 1024 : rl.rlim_max;
      for (int32_t i = 0; i < file_limit; i++) // maybe break when close return EBADF ?
      {
        close(i);
      }

      int fd =open("/dev/null", 0);
      if (fd != -1)
      {
        dup2(fd, 0);
        close(fd);
      }

      TBSYS_LOGGER.setFileName(log_file);

      return pid;
    }

    void Func::hex_dump(const void* data, const int32_t size,
        const bool char_type /*= true*/, const int32_t log_level /*= TBSYS_LOG_LEVEL_DEBUG*/)
    {
      if (TBSYS_LOGGER._level < log_level) return;
      /* dumps size bytes of *data to stdout. Looks like:
       * [0000] 75 6E 6B 6E 6F 77 6E 20
       * 30 FF 00 00 00 00 39 00 unknown 0.....9.
       * (in a single line of course)
       */

      unsigned const char *p = (unsigned char*)data;
      unsigned char c = 0;
      int n = 0;
      char bytestr[4] = {0};
      char addrstr[10] = {0};
      char hexstr[ 16*3 + 5] = {0};
      char charstr[16*1 + 5] = {0};

      for(n = 1; n <= size; n++)
      {
        if (n%16 == 1)
        {
          /* store address for this line */
          snprintf(addrstr, sizeof(addrstr), "%.4x",
              (int)((unsigned long)p-(unsigned long)data) );
        }

        c = *p;
        if (isprint(c) == 0)
        {
          c = '.';
        }

        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

        if (n % 16 == 0)
        {
          /* line completed */
          if (char_type)
            TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%4.4s]   %-50.50s  %s\n",
                addrstr, hexstr, charstr);
          else
            TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%4.4s]   %-50.50s\n",
                addrstr, hexstr);
          hexstr[0] = 0;
          charstr[0] = 0;
        }
        else if(n % 8 == 0)
        {
          /* half line: add whitespaces */
          strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
          strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
      }

      if (strlen(hexstr) > 0)
      {
        /* print rest of buffer if not empty */
        if (char_type)
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%4.4s]   %-50.50s  %s\n",
              addrstr, hexstr, charstr);
        else
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%4.4s]   %-50.50s\n",
              addrstr, hexstr);
      }
    }

    int32_t Func::set_bit(int32_t& data, int32_t index)
    {
      return (data |= (1 << index));
    }

    int32_t Func::clr_bit(int32_t& data, int32_t index)
    {
      return (data &= ~(1 << index));
    }

    int32_t Func::test_bit(int32_t data, int32_t index)
    {
      return (data & (1 << index));
    }

    int64_t Func::get_monotonic_time()
    {
      timespec t;
      clock_gettime(CLOCK_MONOTONIC, &t);
      return t.tv_sec;
    }

    int64_t Func::get_monotonic_time_ms()
    {
      timespec t;
      clock_gettime(CLOCK_MONOTONIC, &t);
      return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000)) ;
    }

    int64_t Func::get_monotonic_time_us()
    {
      timespec t;
      clock_gettime(CLOCK_MONOTONIC, &t);
      return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_nsec/1000));
    }
  }/** common **/
}/** tfs **/
