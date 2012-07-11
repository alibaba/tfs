dnl $Id: config.m4,v 1.1.1.1 2009/01/20 11:14:42 jianbo Exp $
dnl config.m4 for extension tfs_client

PHP_ARG_WITH(tfs_client, for tfs_client support,
[  --with-tfs_client             Include tfs_client support])

if test "$PHP_TFS_CLIENT" != "no"; then
  PHP_SUBST(TFS_CLIENT_SHARED_LIBADD)
  PHP_NEW_EXTENSION(tfs_client, php_tfs_client.cpp, $ext_shared)
  PHP_REQUIRE_CXX

  if test -z "$ROOT"; then
    ROOT=/home/y
  fi

  case $host in
    *linux*)
    ;;
    *freebsd*)
        CC=$ROOT/bin/gcc
        CXX=$ROOT/bin/g++
    ;;
  esac

  PHP_ADD_INCLUDE($ROOT/include)
  PHP_ADD_INCLUDE($TBLIB_ROOT/include/tbsys)
  PHP_ADD_INCLUDE($TBLIB_ROOT/include/tbnet)
  PHP_ADD_INCLUDE($TFS_ROOT/include)

  PHP_ADD_LIBRARY_WITH_PATH(stdc++, /usr/lib, TFS_CLIENT_SHARED_LIBADD)
  TFS_CLIENT_SHARED_LIBADD+="$TAIR_ROOT/lib/libtairclientapi.a $TFS_ROOT/lib/libtfsclient.a $TBLIB_ROOT/lib/libtbnet.a $TBLIB_ROOT/lib/libtbsys.a"
  CPPFLAGS="$CPPFLAGS -g -Werror -Wall"
fi
