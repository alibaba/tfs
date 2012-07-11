<?php
  /**
   *@param rc_ip: rc server_ip
   *@param app_key: application key
   *@param local ip : local ip
   */
	$tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
	echo "\n-------------------\n";
  $data='XXXXXXXXXXXX';
  $data_len = strlen($data);
  echo "\n------------write data case------------\n";
  $fd=$tfsobj->open(NULL,".txt",1);
  echo "\n$fd\n";
  $ret=$tfsobj->write($fd, $data, $data_len);
  echo "\n$ret\n";
  $filename=$tfsobj->close($fd);
  echo "\n$filename[0], $filename[1]\n";
  echo "\n------------read data case------------\n";
  $fd=$tfsobj->open($filename[1],".txt",2);
  $ary=$tfsobj->read($fd);
  echo "\nread data: length: $ary[0], data: $ary[1]------------\n";
  echo "\n------------stat case------------\n";
  $st=$tfsobj->stat($fd);
  echo "\nstat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
  $tfsobj->close($fd);
  echo "\n------------unlink case------------\n";
  $fd=$tfsobj->open($filename[1],".txt",2);
  $tfsobj->unlink($filename[1], ".txt", 0);
  $st=$tfsobj->stat($fd);
  echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
  $tfsobj->close($fd);
?>
