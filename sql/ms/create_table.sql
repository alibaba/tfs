CREATE TABLE t_meta_info (
  app_id bigint not null,
  uid    bigint not null,
  pid    bigint unsigned not null,
  name varbinary(512) not null,
  id   bigint not null,
  create_time datetime not null,
  modify_time datetime not null,
  size bigint not null,
  ver_no smallint default 0,
  meta_info blob,
  PRIMARY KEY (app_id, uid, pid, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
