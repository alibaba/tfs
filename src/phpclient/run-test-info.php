<?php echo "
PHP_SAPI    : " , PHP_SAPI , "
PHP_VERSION : " , phpversion() , "
ZEND_VERSION: " , zend_version() , "
PHP_OS      : " , PHP_OS , " - " , php_uname() , "
INI actual  : " , realpath(get_cfg_var("cfg_file_path")) , "
More .INIs  : " , (function_exists('php_ini_scanned_files') ? str_replace("\n","", php_ini_scanned_files()) : "** not determined **"); ?>