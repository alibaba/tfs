CREATE TABLE  tfs_sequence (
  `name` varchar(50) NOT NULL,
  `current_value` bigint NOT NULL,
  `increment` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into tfs_sequence (name, current_value) values ('seq_pid', 1);

delimiter $$
CREATE PROCEDURE  pid_seq_nextval()
BEGIN
  declare cur_val bigint;
  START TRANSACTION;
  UPDATE tfs_sequence
  SET current_value = current_value + increment where name = 'seq_pid';
  select current_value - increment into cur_val from tfs_sequence where name = 'seq_pid';
  commit;
  select cur_val;
END $$
delimiter ;

