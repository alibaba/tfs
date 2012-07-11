#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

extern "C"
{
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
}

#include "php_tfs_client.h"
#include "fsname.h"
#include "tfs_rc_client_api.h"
#include <tbsys.h>
#include <tbnet.h>
#include <vector>

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

/* True global resources - no need for thread safety here */
static zend_class_entry *tfs_client_class_entry_ptr;
static RcClient* gclient;

/* {{{ tfs_client_functions[]
 *
 * Every user visible function must have an entry in tfs_client_functions[].
 */
function_entry tfs_client_functions[] = {
	PHP_FE(confirm_tfs_client_compiled,	NULL)		/* For testing, remove later. */
	{NULL, NULL, NULL}	/* Must be the last line in tfs_client_functions[] */
};
/* }}} */

static zend_function_entry tfs_client_class_functions[] = {
	PHP_FE(tfs_client, NULL)
	PHP_FALIAS(write, tfs_client_write, NULL)
	PHP_FALIAS(open, tfs_client_open, NULL)
	PHP_FALIAS(close, tfs_client_close, NULL)
	PHP_FALIAS(read, tfs_client_read, NULL)
	PHP_FALIAS(stat, tfs_client_stat, NULL)
	PHP_FALIAS(unlink, tfs_client_unlink, NULL)
	PHP_FALIAS(fopen, tfs_client_fopen, NULL)
	PHP_FALIAS(create_dir, tfs_client_create_dir, NULL)
	PHP_FALIAS(create_file, tfs_client_create_file, NULL)
	PHP_FALIAS(rm_dir, tfs_client_rm_dir, NULL)
	PHP_FALIAS(rm_file, tfs_client_rm_file, NULL)
	PHP_FALIAS(mv_dir, tfs_client_mv_dir, NULL)
	PHP_FALIAS(mv_file, tfs_client_mv_file, NULL)
	PHP_FALIAS(get_app_id, tfs_client_get_app_id, NULL)
	PHP_FALIAS(pwrite, tfs_client_pwrite, NULL)
	PHP_FALIAS(pread, tfs_client_pread, NULL)
	PHP_FALIAS(fstat, tfs_client_fstat, NULL)
	PHP_FALIAS(ls_dir, tfs_client_ls_dir, NULL)
	{NULL, NULL, NULL}
};

/* {{{ tfs_client_module_entry
 */
zend_module_entry tfs_client_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	"tfs_client",
	tfs_client_functions,
	PHP_MINIT(tfs_client),
	PHP_MSHUTDOWN(tfs_client),
	NULL,
	NULL,
	PHP_MINFO(tfs_client),
#if ZEND_MODULE_API_NO >= 20010901
	"$", /* Replace with version number for your extension */
#endif
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

extern "C"
{
#ifdef COMPILE_DL_TFS_CLIENT
    ZEND_GET_MODULE(tfs_client)
#endif
}

/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(tfs_client)
{
	/*** Defines TFS Class ***/
  static zend_class_entry tfs_client_class_entry;
	INIT_CLASS_ENTRY( tfs_client_class_entry, "tfs_client", tfs_client_class_functions );
	tfs_client_class_entry_ptr = zend_register_internal_class( &tfs_client_class_entry TSRMLS_CC);
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(tfs_client)
{
  if (NULL != gclient)
  {
    delete gclient;
  }
	return SUCCESS;
}
/* }}} */


/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(tfs_client)
{
	php_info_print_table_start();
	php_info_print_table_row(2, "tfs_client support", "enabled");
	php_info_print_table_row(2, "Version", "2.0.0");
	php_info_print_table_end();
}
/* }}} */

/* {{{ $tfsclient = new tfs_client(const char* rc_ip, const char* app_key, const char* app_ip)
 *
 * Constructor for tfs_client class
 */
PHP_FUNCTION(tfs_client)
{
  char* app_ip = NULL;
  char* app_key = NULL;
  char* rc_ip  = NULL;
  long app_ip_length = 0;
  long app_key_length = 0;
  long rc_ip_length = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sss",
            &rc_ip, &rc_ip_length, &app_key, &app_key_length, &app_ip, &app_ip_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == app_ip || NULL == rc_ip || NULL == app_key
                  || app_ip_length <= 0 || rc_ip_length <= 0 || app_key_length <= 0 ? FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    gclient = new RcClient();
    ret = gclient->initialize(rc_ip, app_key, app_ip);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: initialize failed, ret: %d", ret);
    }
    ret = ret == TFS_SUCCESS ? SUCCESS : FAILURE;
  }
  if (SUCCESS != ret)
    RETURN_NULL();
}
/* }}} */

/* {{{ int $tfs_client->open(const char* filename, const char* suffix, int mode, bool large, const char* local_key)
 *  open a file via tfs_client
 */
PHP_FUNCTION(tfs_client_open)
{
  char* filename = NULL;
  char* suffix   = NULL;
  char* local_key= NULL;
  long file_name_length = 0;
  long suffix_length = 0;
  long mode = -1;
  long local_key_length = 0;
  long fd = -1;
  bool    large = false;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s!s!l|bs!",
            &filename, &file_name_length, &suffix, &suffix_length , &mode, &large, &local_key, &local_key_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = mode < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    fd = gclient->open(filename, suffix, static_cast<RcClient::RC_MODE>(mode), large, local_key);
    ret = fd > 0 ? SUCCESS : FAILURE;
    if (SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: open file %s failed, ret: %ld", filename, fd);
    }
  }
  RETURN_LONG(fd);
}
/* }}} */

/* {{{ int $tfs_client->fopen(const long app_id, const long uid, const char* file_path,int mode)
 *  open a file via tfs_client
 */
PHP_FUNCTION(tfs_client_fopen)
{
  long app_id = -1;
  long uid = -1;
  char* filename = NULL;
  long file_name_length = 0;
  long mode = -1;
  long fd   = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "llsl",
            &app_id, &uid, &filename, &file_name_length, &mode);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == filename || mode < 0 || app_id < 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    fd = gclient->open(app_id, uid, filename, static_cast<RcClient::RC_MODE>(mode));
    ret = fd > 0 ? SUCCESS : FAILURE;
    if (SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: open file %s failed, ret: %ld", filename, fd);
    }
  }
  RETURN_LONG(fd);
}
/* }}} */


/* {{{ int $tfs_client->close(const int fd, char* ret_tfs_file_name, const ret_tfs_file_name_length)
 * close tfs file via tfs_client
 */
PHP_FUNCTION(tfs_client_close)
{
  long fd = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l",
            &fd);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = fd <= 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  array_init(return_value);
  if (SUCCESS == ret)
  {
    char name[MAX_FILE_NAME_LEN];
    ret = gclient->close(fd, name, MAX_FILE_NAME_LEN);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: close file failed, fd: %ld, ret: %d", fd, ret);
    }
    else
    {
      add_next_index_long(return_value, strlen(name));
      add_next_index_string(return_value, name, 1);
    }
  }
  if (SUCCESS != ret)
  {
    add_next_index_long(return_value, 0);
  }
}
/* }}} */

/* {{{ int $tfs_client->write(const int fd, const char* data, const int length)
 * write data to tfs via tfs_client
 */
PHP_FUNCTION(tfs_client_write)
{
  char* data = NULL;
  long data_length = 0;
  long length = -1;
  long fd = -1;
  long ret_length = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsl",
            &fd, &data, &data_length, &length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = fd <= 0 || NULL == data || length <= 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret_length = gclient->write(fd, data, length);
    ret = ret_length <= 0 ? FAILURE : SUCCESS;
    if (ret_length <= 0)
    {
		  php_error(E_ERROR, "tfs_client: write data failed, fd: %ld, ret: %ld", fd, ret_length);
    }
  }
  RETURN_LONG(ret_length);
}
/* }}} */

/* {{{ int $tfs_client->read(const int fd);
 * read data form tfs
 */
PHP_FUNCTION(tfs_client_read)
{
  long fd = -1;
  long count = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ll", &fd, &count);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = fd <= 0  || count <= 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  long offset = -1;
  array_init(return_value);
  if (SUCCESS == ret)
  {
    char* data = (char*)emalloc(count);
    ret = NULL == data ? FAILURE : SUCCESS;
    if (SUCCESS == ret)
    {
      offset = gclient->read(fd, data, count);
      ret = offset > 0 ? SUCCESS : FAILURE;
      if (SUCCESS != ret)
      {
        php_error(E_WARNING, "read data failed, fd: %ld, count: %ld", fd, offset);
      }
      else
      {
        add_next_index_long(return_value, offset);
        add_next_index_stringl(return_value, data, offset, 1);
      }
      efree(data);
      data = NULL;
    }
  }
  if (SUCCESS != ret)
  {
    add_next_index_long(return_value, offset);
  }
}
/* }}} */

/* {{{ Array $tfs_client->stat(const int fd)
 * Does a tfs_client get tfsfile stat
 */
PHP_FUNCTION(tfs_client_stat)
{
  long fd = -1;
  array_init(return_value);
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l",
            &fd);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = fd <= 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }

  TfsFileStat finfo;
  if (SUCCESS == ret)
  {
    ret = gclient->fstat(fd, &finfo);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: stat failed, fd: %ld, ret: %d", fd, ret);
    }
    ret = ret == TFS_SUCCESS ? SUCCESS  : FAILURE;
  }
  if (SUCCESS == ret)
  {
	  char tmpStr[128];
    add_next_index_string(return_value, (char*)(boost::lexical_cast<string>(finfo.size_).c_str()), 1);
    add_next_index_string(return_value,(char*)(tbsys::CTimeUtil::timeToStr(finfo.modify_time_,tmpStr)), 1);
    add_next_index_string(return_value,(char*)(tbsys::CTimeUtil::timeToStr(finfo.create_time_,tmpStr)), 1);
    add_next_index_string(return_value, (char*)(boost::lexical_cast<string>(finfo.crc_).c_str()), 1);
  }
  if (SUCCESS != ret)
    RETURN_FALSE;
}
/* }}} */

/* {{{ bool $tfs_client->unlink(const char* filename, const char* suffix, const int action)
 *
 * Does a tfs_client remove file via tfsfile
 */
PHP_FUNCTION(tfs_client_unlink)
{
  char* filename = NULL;
  char* suffix   = NULL;
  long file_name_length = 0;
  long suffix_length = 0;
  long action = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss!l",
            &filename, &file_name_length, &suffix, &suffix_length, &action);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == filename || action < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }

  if (SUCCESS == ret)
  {
    ret = gclient->unlink(filename, suffix, static_cast<TfsUnlinkType>(action));
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: remove file %s, ret: %d", filename, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->create_dir(const int uid, const char* dir_path)
 * create directory
 */
PHP_FUNCTION(tfs_client_create_dir)
{
  char* dir_path = NULL;
  long dir_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls",
            &uid, &dir_path, &dir_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == dir_path || dir_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->create_dir(uid, dir_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: create directory %s failed, ret: %d", dir_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->create_file(const int uid, const char* file_path)
 * create file
 */
PHP_FUNCTION(tfs_client_create_file)
{
  char* file_path = NULL;
  long file_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls",
            &uid, &file_path, &file_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == file_path || file_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->create_file(uid, file_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: create file %s failed, ret: %d", file_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->rm_dir(const int uid, const char* dir_path)
 * rm directory
 */
PHP_FUNCTION(tfs_client_rm_dir)
{
  char* dir_path = NULL;
  long dir_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls",
            &uid, &dir_path, &dir_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == dir_path || dir_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->rm_dir(uid, dir_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: remove directory %s failed, ret: %d", dir_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->rm_file(const int uid, const char* file_path)
 * rm file
 */
PHP_FUNCTION(tfs_client_rm_file)
{
  char* file_path = NULL;
  long file_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls",
            &uid, &file_path, &file_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == file_path || file_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->rm_file(uid, file_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: remove file %s failed, ret: %d", file_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->mv_dir(const int uid, const char* src_dir_path, const char* dest_dir_path)
 * mv directory
 */
PHP_FUNCTION(tfs_client_mv_dir)
{
  char* dir_path = NULL;
  long dir_path_length = 0;
  char* dest_dir_path = NULL;
  long dest_dir_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lss",
            &uid, &dir_path, &dir_path_length, &dest_dir_path, &dest_dir_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == dir_path || dir_path_length <= 0  || NULL == dest_dir_path || dest_dir_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->mv_dir(uid, dir_path, dest_dir_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: mv directory %s to %s failed, ret: %d", dir_path, dest_dir_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ $tfs_client->mv_file(const int uid, const char* src_file_path, const char* dest_file_path)
 * move file
 */
PHP_FUNCTION(tfs_client_mv_file)
{
  char* file_path = NULL;
  long file_path_length = 0;
  char* dest_file_path = NULL;
  long dest_file_path_length = 0;
  long uid = 0;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lss",
            &uid, &file_path, &file_path_length, &dest_file_path, &dest_file_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == file_path || file_path_length <= 0  || NULL == dest_file_path || dest_file_path_length <= 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->mv_dir(uid, file_path, dest_file_path);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: mv file %s to %s failed, ret: %d", file_path, dest_file_path, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */


/* {{{ $tfs_client->pread(const int fd, const int length, const int offset)
 * create directory
 */
PHP_FUNCTION(tfs_client_pread)
{
  long fd = -1;
  long length = 0;
  long offset = 0;
  long ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lll",
            &fd, &length, &offset);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = length <= 0 || fd < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  long off = -1;
  array_init(return_value);
  if (SUCCESS == ret)
  {
    char* data = (char*)emalloc(length);
    ret = NULL == data ? FAILURE : SUCCESS;
    if (SUCCESS == ret)
    {
      off = gclient->pread(fd, data, length, offset);
      ret = off > 0 ? SUCCESS: FAILURE;
      if (SUCCESS != ret)
      {
		    php_error(E_ERROR, "tfs_client: read data failed, fd: %ld ret: %"PRI64_PREFIX"d", fd, off);
      }
      else
      {
        add_next_index_long(return_value, off);
        add_next_index_stringl(return_value, data, off, 1);
      }
      efree(data);
      data = NULL;
    }
  }
  if (SUCCESS != ret)
    add_next_index_long(return_value, off);
}
/* }}} */

/* {{{ $tfs_client->pwrite(const int fd, const char* buf, const int length, const int offset)
 * create directory
 */
PHP_FUNCTION(tfs_client_pwrite)
{
  long fd = -1;
  char* data= NULL;
  long data_length = 0;
  long length = 0;
  long offset = 0;
  long ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsll",
            &fd, &data, &data_length, &length, &offset);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = NULL == data || length <= 0 || fd < 0 || offset < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    ret = gclient->pwrite(fd, data, length, offset);
    if (ret < 0)
    {
		  php_error(E_ERROR, "tfs_client: write data failed, fd: %ld ret: %ld", fd, ret);
    }
  }
  RETURN_LONG(ret);
}
/* }}} */

/* {{{ long $tfs_client->get_app_id()
 * get app id
 */
PHP_FUNCTION(tfs_client_get_app_id)
{
  RETURN_LONG(gclient->get_app_id());
}
/* }}} */

/* {{{ int $tfs_client->fstat(const long app_id, const long uid, const char* file_path)
 * stat file
 */
PHP_FUNCTION(tfs_client_fstat)
{
  char* file_path = NULL;
  long app_id = -1;
  long uid    = -1;
  long file_path_length = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lls",
            &app_id, &uid, &file_path, &file_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = app_id < 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    FileMetaInfo info;
    ret = gclient->ls_file(app_id, uid, file_path, info);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: fstat %s failed, ret: %d", file_path, ret);
    }
    else
    {
      char tmpStr[64];
      array_init(return_value);
      add_next_index_string(return_value, (char*)(boost::lexical_cast<string>(info.size_).c_str()), 1);
      add_next_index_string(return_value, (char*)(tbsys::CTimeUtil::timeToStr(info.modify_time_,tmpStr)), 1);
      add_next_index_string(return_value, (char*)(tbsys::CTimeUtil::timeToStr(info.create_time_,tmpStr)), 1);
    }
    ret = ret == TFS_SUCCESS ? SUCCESS : FAILURE;
  }
  if (SUCCESS != ret)
    RETURN_FALSE;
}
/* }}} */

/* {{{ mixed $tfs_client->ls_dir(const long app_id, const long uid, const char* file_path)
 * ls directory
 */
PHP_FUNCTION(tfs_client_ls_dir)
{
  char* file_path = NULL;
  long app_id = -1;
  long uid    = -1;
  long file_path_length = -1;
  int32_t ret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lls",
            &app_id, &uid, &file_path, &file_path_length);
  if (FAILURE == ret)
  {
		php_error(E_WARNING, "tfs_client: can't parse parameters");
  }
  if (SUCCESS == ret)
  {
    ret = app_id < 0 || uid < 0 ?  FAILURE : SUCCESS;
    if (SUCCESS != ret)
    {
		  php_error(E_WARNING, "tfs_client: parameters is invalid");
    }
  }
  if (SUCCESS == ret)
  {
    std::vector<FileMetaInfo> vinfo;
    ret = gclient->ls_dir(app_id, uid, file_path, vinfo);
    if (TFS_SUCCESS != ret)
    {
		  php_error(E_ERROR, "tfs_client: ls %s failed, ret: %d", file_path, ret);
    }
    else
    {
      char tmpStr[64];
      array_init(return_value);
      std::vector<FileMetaInfo>::const_iterator iter = vinfo.begin();
      for (; iter != vinfo.end(); ++iter)
      {
        add_next_index_string(return_value, (char*)((*iter).name_.c_str()), 1);
        add_next_index_string(return_value, (char*)(boost::lexical_cast<string>((*iter).size_).c_str()), 1);
        add_next_index_string(return_value, (char*)(boost::lexical_cast<string>((*iter).ver_no_).c_str()), 1);
        add_next_index_string(return_value, (char*)(tbsys::CTimeUtil::timeToStr((*iter).create_time_,tmpStr)), 1);
        add_next_index_string(return_value, (char*)(tbsys::CTimeUtil::timeToStr((*iter).modify_time_,tmpStr)), 1);
      }
    }
    ret =  ret == TFS_SUCCESS ? SUCCESS : FAILURE;
  }
  if (SUCCESS != ret)
    RETURN_FALSE;
}
/* }}} */

/* Remove the following function when you have succesfully modified config.m4
   so that your module can be compiled into PHP, it exists only for testing
   purposes. */

/* Every user-visible function in PHP should document itself in the source */
/* {{{ proto string confirm_tfs_client_compiled(string arg)
   Return a string to confirm that the module is compiled in */
PHP_FUNCTION(confirm_tfs_client_compiled)
{
	char *arg = NULL;
	int arg_len, len;
	char string[256];

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &arg, &arg_len) == FAILURE) {
		return;
	}

	len = sprintf(string, "Congratulations! You have successfully modified ext/%.78s/config.m4. Module %.78s is now compiled into PHP.", "tfs_client", arg);
	RETURN_STRINGL(string, len, 1);
}
/* }}} */
/* The previous line is meant for vim and emacs, so it can correctly fold and
   unfold functions in source code. See the corresponding marks just before
   function definition, where the functions purpose is also documented. Please
   follow this convention for the convenience of others editing your code.
*/
