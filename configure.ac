#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT([tfs], 2.2.0, duolong@taobao.com)
AC_CONFIG_SRCDIR([src/common/config_item.h])

AM_INIT_AUTOMAKE(tfs, 2.2.0)
# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_PROG_LIBTOOL

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lc':
AC_CHECK_LIB([c], [main])
# FIXME: Replace `main' with a function in `-ldl':
AC_CHECK_LIB([dl], [main])
# FIXME: Replace `main' with a function in `-lm':
AC_CHECK_LIB([m], [main])
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread], [main])
# FIXME: Replace `main' with a function in `-lrt':
AC_CHECK_LIB([rt], [main])
# FIXME: Replace `main' with a function in `-ltcmalloc':
#AC_CHECK_LIB([tcmalloc], [main])

AC_CHECK_LIB([mysqlclient], [main])

AC_ARG_WITH([tblib-root],
           AS_HELP_STRING([--with-tblib-root],
           [tblib installation path]),
           TBLIB_ROOT=${withval})
if  test -z "${TBLIB_ROOT}"; then
   dnl tbnet include path supplied
   TBLIB_ROOT="/usr/local"
fi
if ! test -d "${TBLIB_ROOT}/include" -a -d "${TBLIB_ROOT}/lib"; then
        AC_MSG_ERROR([tblib_root is not work! expect tblib_root/include and tblib_root/lib directory])
        exit 1;
fi
AC_SUBST([TBLIB_ROOT])

AC_ARG_ENABLE([readline],
           AS_HELP_STRING([--disable-readline],
           [disable tfstool autocomplete & history feature (default enable,need gnu readline and ncurses surpport)]),
           [enable_readline=${enableval}],
           [enable_readline="yes"])
if test "${enable_readline}" = "yes"; then
    AC_CHECK_HEADERS(readline/readline.h readline/history.h,
                     [READLINE_INCLUDE="-D_WITH_READ_LINE"],
                     [AC_MSG_ERROR([readline header files not found, --disable-readline or install gnu readline library.
                      --help for more information.])],)
    AC_CHECK_LIB(termcap,main,
                 [READLINE_LIB="${READLINE_LIB} -ltermcap"],
                 [AC_MSG_ERROR([termcap libarary not found,--disable-readline or install libncurses library.
                  --help for more information.])],)
    AC_CHECK_LIB(readline,main,
                 [READLINE_LIB="${READLINE_LIB} -lreadline"],
                 [AC_MSG_ERROR([readline library not found, --disable-readline or install gnu readline library.
                  --help for more information.])],[-ltermcap])
fi
AC_SUBST([READLINE_INCLUDE])
AC_SUBST([READLINE_LIB])

AC_ARG_WITH([tair-root],
           AS_HELP_STRING([--with-tair-root],
           [Tair installation path, need when enable uniquestore feature]),
           TAIR_ROOT=${withval})
if  test -z "${TAIR_ROOT}"; then
   dnl tbnet include path supplied
   TAIR_ROOT="/usr/local"
fi

AC_ARG_ENABLE([uniquestore],
           AS_HELP_STRING([--enable-uniquestore],
           [enable unique store feature. need TaoBao Tair library support, use --with-tair-root to specify path]),
           [enable_uniquestore=${enableval}],
           [enable_uniquestore="no"])
if test "${enable_uniquestore}" = "yes"; then
        if ! test -d "${TAIR_ROOT}/include" -a -d "${TBLIB_ROOT}/lib"; then
                AC_MSG_ERROR([TAIR_ROOT is not work! expect TAIR_ROOT/include and TAIR_ROOT/lib directory])
                exit 1;
        fi
        UNIQUE_STORE_CPPFLAGS="-DWITH_UNIQUE_STORE -I${TAIR_ROOT}/include"
        UNIQUE_STORE_LDFLAGS="${TAIR_ROOT}/lib/libtairclientapi.a -lboost_thread -lz"
fi
AM_CONDITIONAL([WITH_UNIQUE_STORE],[test x$enable_uniquestore = xyes])
AC_SUBST([UNIQUE_STORE_CPPFLAGS])
AC_SUBST([UNIQUE_STORE_LDFLAGS])

AC_ARG_ENABLE([taircache],
           AS_HELP_STRING([--enable-taircache],
           [enable tair cache feature. need TaoBao Tair library support, use --with-tair-root to specify path]),
           [enable_taircache=${enableval}],
           [enable_taircache="no"])
if test "${enable_taircache}" = "yes"; then
        if ! test -d "${TAIR_ROOT}/include" -a -d "${TBLIB_ROOT}/lib"; then
                AC_MSG_ERROR([TAIR_ROOT is not work! expect TAIR_ROOT/include and TAIR_ROOT/lib directory])
                exit 1;
        fi
        TAIR_CACHE_CPPFLAGS="-DWITH_TAIR_CACHE -I${TAIR_ROOT}/include"
        TAIR_CACHE_LDFLAGS="${TAIR_ROOT}/lib/libtairclientapi.a -lboost_thread -lz"
fi
AM_CONDITIONAL([WITH_TAIR_CACHE],[test x$enable_taircache = xyes])
AC_SUBST([TAIR_CACHE_CPPFLAGS])
AC_SUBST([TAIR_CACHE_LDFLAGS])

AC_CACHE_CHECK(if fallocate is implemented, ac_cv_func_fallocate,
  AC_TRY_RUN([
    #include <fcntl.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <unistd.h>
    int main()
    {
      char* file = "./test_fallocate";
      int fd = open(file, O_RDWR | O_CREAT, 0666);
      if (fd < 0)
      {
        exit(1);
      }
      fallocate(fd, 0, 0, 512);
      close(fd);
      unlink(file);
      exit(0);
    }
  ],ac_cv_func_fallocate="yes",ac_cv_func_fallocate="no")
)
if test "$ac_cv_func_fallocate" = "yes" ; then
  AC_DEFINE_UNQUOTED(TFS_DS_FALLOCATE, 1, [ac_cv_func_fallocate])
else
  AC_DEFINE_UNQUOTED(TFS_DS_FALLOCATE, 0, [ac_cv_func_fallocate])
fi

if test "$ac_cv_set_fallocate" = "no" ; then
  AC_MSG_ERROR([fallocate does not work.])
        exit 1;
fi

AC_ARG_WITH([release],
  AS_HELP_STRING([--with-release],
    [use optimize (default is NO)]),
    [
      if test "$withval" = "yes"; then
        CXXFLAGS="-O2 -finline-functions -fno-strict-aliasing -Wall -fPIC -D__STDC_LIMIT_MACROS -D_NO_EXCEPTION"
      fi
    ],
    [CXXFLAGS="-g -D__STDC_LIMIT_MACROS -Wall -Werror -Wextra -Wunused-parameter -Wformat -Wconversion -Wdeprecated "]
    #[CXXFLAGS="-g -Wall -fPIC -D__STDC_LIMIT_MACROS -DTFS_TEST -D_NO_EXCEPTION"]
)

ac_default_prefix=${HOME}/tfs_bin
# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stdint.h stdlib.h string.h sys/ioctl.h sys/socket.h sys/time.h sys/vfs.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_ERROR_AT_LINE
AC_PROG_GCC_TRADITIONAL
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_CHECK_FUNCS([fdatasync ftruncate gethostbyname getpagesize gettimeofday memset mkdir munmap rmdir socket strcasecmp strchr strdup strerror strstr strtoul strtoull tzset])

m4_include([tcmalloc.m4])

m4_include([ax_lib_mysql.m4])

AX_LIB_MYSQL([5.1.48])

TS_CHECK_TCMALLOC

AC_CONFIG_FILES([Makefile
                 conf/Makefile
                 sql/Makefile
                 sql/ms/Makefile
                 sql/rcs/Makefile
                 scripts/Makefile
                 scripts/ha/Makefile
                 src/Makefile
                 src/adminserver/Makefile
                 src/new_client/Makefile
                 src/common/Makefile
                 src/dataserver/Makefile
                 src/message/Makefile
                 src/monitor/Makefile
                 src/nameserver/Makefile
                 src/rcserver/Makefile
                 src/name_meta_server/Makefile
                 src/rootserver/Makefile
                 src/checkserver/Makefile
                 src/tools/Makefile
                 src/tools/util/Makefile
                 src/tools/dataserver/Makefile
                 src/tools/nameserver/Makefile
                 src/tools/adminserver/Makefile
                 src/tools/transfer/Makefile
                 src/tools/mock/Makefile
                 src/tools/cluster/Makefile
                 tests/Makefile
                 tests/batch/Makefile
                 tests/rcserver/Makefile
                 tests/name_meta_server/Makefile
                 tests/common/Makefile
                 tests/rootserver/Makefile
                 tests/dataserver/Makefile
                 tests/intergrate/Makefile])
AC_OUTPUT
