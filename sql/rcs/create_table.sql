CREATE TABLE t_resource_server_info (
  addr_info VARCHAR(64) NOT NULL,
  stat INT NOT NULL DEFAULT 1,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (addr_info)
);

CREATE TABLE t_meta_root_info (
  app_id INT,
  addr_info VARCHAR(64) NOT NULL,
  stat INT NOT NULL DEFAULT 1,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (app_id)
);

CREATE TABLE t_cluster_rack_info (
  cluster_rack_id INT,
  cluster_id   VARCHAR(8),
  ns_vip VARCHAR(64),
  cluster_stat  INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_rack_id, cluster_id)
);


CREATE TABLE t_cluster_rack_group (
  cluster_group_id INT,
  cluster_rack_id INT,
  cluster_rack_access_type INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_group_id, cluster_rack_id)
);

CREATE TABLE t_cluster_rack_duplicate_server (
  cluster_rack_id INT,
  dupliate_server_addr VARCHAR(64),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_rack_id)
);

CREATE TABLE t_base_info_update_time (
  base_last_update_time DATETIME,
  app_last_update_time  DATETIME
);

CREATE TABLE t_app_info (
  app_key VARCHAR(255),
  id INT,
  quto BIGINT,
  cluster_group_id INT,
  use_remote_cache INT default 0,
  app_name VARCHAR(255) NOT NULL,
  app_owner VARCHAR(255),
  report_interval INT,
  need_duplicate INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (app_key)
);

ALTER TABLE t_app_info ADD UNIQUE KEY app_info_un_id (id);
ALTER TABLE t_app_info ADD UNIQUE KEY app_info_un_name (app_name);

CREATE TABLE t_session_info (
  session_id VARCHAR(255),
  cache_size BIGINT,
  cache_time BIGINT,
  client_version VARCHAR(64),
  log_out_time DATETIME,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (session_id)
);

CREATE TABLE t_session_stat (
  session_id VARCHAR(255),
  oper_type  INT,
  oper_times BIGINT,
  file_size BIGINT,
  response_time INT,
  succ_times BIGINT,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (session_id, oper_type)
);

CREATE TABLE t_app_stat (
  app_id INT,
  used_capacity BIGINT,
  file_count BIGINT,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (app_id)
);

CREATE TABLE t_caculate_ip_info (
  source_ip VARCHAR(64),
  caculate_ip VARCHAR(64),
  PRIMARY KEY (source_ip)
);
CREATE TABLE t_app_ip_replace (
  app_id INT,
  source_ip VARCHAR(64),
  turn_ip VARCHAR(64),
  PRIMARY KEY (app_id, source_ip)
);

CREATE TABLE  t_cluster_cache_info (
  cache_server_addr VARCHAR(128),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cache_server_addr)
);
