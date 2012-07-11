<?php
  /**
   *@param rc_ip: rc server_ip
   *@param app_key: application key
   *@param local ip : local ip
   */
	$tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
	echo "\n-------------------\n";
  $app_id = $tfsobj->get_app_id();
  $uid = 100;
  $dir_path="/";
  echo "\n app_id: $app_id\n";
  echo "\n------------create dir case------------\n";
  //$tfsobj->create_dir($uid, $dir_path);
  $file_path="/tkkk22222kkkkkk";
  $tfsobj->create_file($uid, $file_path);

  echo "\n------------open file case------------\n";
  $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
  echo "\nfd: $fd\n";
  
  echo "\n------------write file case------------\n";
  $data='XXXXXXXXXXXX';
  $data_len = strlen($data);
  $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
  echo "\n write data ret: $ret\n";

  echo "\n------------read file case------------\n";
  $ret=$tfsobj->pread($fd, $data_len, 0);
  echo "\n read data ret: length : $ret[0], data: $ret[1]\n";

  echo "\n------------rm file case------------\n";
  $ret = $tfsobj->rm_file($uid, $file_path);
  echo "\nrm_file case ret: $ret------------\n";

  echo "\n------------create dir case------------\n";
  $dir_path="/test_dir_00000";
  $ret = $tfsobj->create_dir($uid, $dir_path);
  echo "\ncreate dir case ret: $ret------------\n";

  echo "\n------------rm dir case------------\n";
  $ret = $tfsobj->rm_dir($uid, $dir_path);
  echo "\nrm dir case ret: $ret------------\n";

  echo "\n------------mv dir case------------\n";
  $dir_path="/test_dir_100000003200010";
  $dir_path1="/test_dir_2000000320010";
  $ret = $tfsobj->create_dir($uid, $dir_path1);
  $ret = $tfsobj->mv_dir($uid, $dir_path1, $dir_path);

  echo "\nmv dir case ret: $ret------------\n";
  $ret = $tfsobj->rm_dir($uid, $dir_path);

  echo "\n------------fstat case------------\n";
  $file_path="/test_file_stat";
  $tfsobj->create_file($uid, $file_path);
  $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
  $data='XXXXXXXXXXXX';
  $data_len = strlen($data);
  $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
  $ary=$tfsobj->fstat($tfsobj->get_app_id(), $uid, $file_path);
  echo "\n fstat ret: $ary[0], $ary[1], $ary[2] =====\n";
  $ret = $tfsobj->rm_file($uid, $file_path);

  echo "\n------------ls dir case------------\n";
  $dir_path="/t1ss";
  $tfsobj->create_dir($uid, $dir_path);
  $tfsobj->create_dir($uid, $dir_path."/test1");
  $tfsobj->create_dir($uid, $dir_path."/test2");
  $tfsobj->create_dir($uid, $dir_path."/test3");
  $ary=$tfsobj->ls_dir($tfsobj->get_app_id(), $uid, $dir_path);
  echo "\n ls dir ret: $ary[0], $ary[1], $ary[2]------------\n";
  $ret = $tfsobj->rm_dir($uid, $dir_path."/test1");
  $ret = $tfsobj->rm_dir($uid, $dir_path."/test2");
  $ret = $tfsobj->rm_dir($uid, $dir_path."/test3");
  $ret = $tfsobj->rm_dir($uid, $dir_path);
?>
