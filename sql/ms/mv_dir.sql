drop procedure mv_dir;
delimiter $$
create procedure
mv_dir(in i_app_id bigint, in i_uid bigint,
  in i_s_ppid bigint unsigned, in i_s_pid bigint unsigned, in i_s_pname varbinary(512),
  in i_d_ppid bigint unsigned, in i_d_pid bigint unsigned, in i_d_pname varbinary(512),
  in i_s_name varbinary(512), in i_d_name varbinary(512))
begin
  declare aff_row int;
  declare o_ret bigint;
  declare exit handler for sqlexception
  begin
    set o_ret = -14000;
    rollback;
    select o_ret;
  end;
  select 0 into aff_row;
  select -14000 into o_ret;
  start transaction;
  update t_meta_info set modify_time = now()
  where app_id = i_app_id and uid = i_uid and pid = i_s_ppid
  and name = i_s_pname and id = i_s_pid;
  select count(1) into aff_row from t_meta_info
  where app_id = i_app_id and uid = i_uid and pid = i_s_ppid
  and name = i_s_pname and id = i_s_pid;
  if aff_row = 1 then
    if i_s_ppid <> i_d_ppid or i_s_pname <> i_d_pname or i_s_pid <> i_d_pid then
      update t_meta_info set modify_time = now()
      where app_id = i_app_id and uid = i_uid and pid = i_d_ppid
      and name = i_d_pname and id = i_d_pid;
      select count(1) into aff_row from t_meta_info
      where app_id = i_app_id and uid = i_uid and pid = i_d_ppid
      and name = i_d_pname and id = i_d_pid;
    end if;
    if aff_row = 1 then
      update t_meta_info set pid = i_d_pid, name = i_d_name, modify_time = now()
      where app_id = i_app_id and uid = i_uid
      and pid = i_s_pid and name = i_s_name;
      select row_count() into aff_row;
      if aff_row = 1 then
        set o_ret = 1;
      else
        set o_ret = -14001;
      end if;
    else
      set o_ret = -14002;
    end if;
  else
    set o_ret = -14002;
  end if;
  if o_ret <= 0 then
    rollback;
  else
    commit;
  end if;
  select o_ret;
end $$
delimiter ;
