
insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, 
  app_owner, report_interval, need_duplicate, rem, create_time, modify_time)
values ('tappkey00001', 1, 1024*1024*1024, 1, 'picture_space',
  'mutang', 5, 1, '', now(), now());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, 
  app_owner, report_interval, need_duplicate, rem, create_time, modify_time)
values ('tappkey00002', 2, 1024*1024*1024, 1, 'ic',
  'xxxx', 5, 1, '', now(), now());

insert into t_base_info_update_time (base_last_update_time, app_last_update_time) 
values (now(), now());

insert into t_cluster_rack_duplicate_server
(cluster_rack_id, dupliate_server_addr, create_time, modify_time)
values
(1, '10.232.35.40:81000', now(), now());

insert into t_cluster_rack_group 
(cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time)
values
(1, 1, 1, '', now(), now());

insert into t_cluster_rack_group 
(cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time)
values
(1, 2, 1, '', now(), now());

insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(1, 'T1Mxxxx', '10.232.35.41:1123', 1, '', now(), now());
insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(1, 'T5Mxxxx', '10.232.35.42:1123', 1, '', now(), now());

insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(2, 'T1Bxxxx', '10.232.35.41:2123', 1, '', now(), now());
insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(2, 'T5Bxxxx', '10.232.35.42:2123', 1, '', now(), now());

insert into t_resource_server_info (addr_info, stat, rem, create_time, modify_time)
values ('10.232.35.40:90000', 1, 'test first', now(), now());
insert into t_resource_server_info (addr_info, stat, rem, create_time, modify_time)
values ('10.232.35.40:91000', 1, 'test_second', now(), now());

