drop procedure mv_file;
delimiter $$
create procedure
mv_file(in i_app_id bigint, in i_uid bigint,
  in i_s_ppid bigint unsigned, in i_s_pid bigint unsigned, in i_s_pname varbinary(512),
  in i_d_ppid bigint unsigned, in i_d_pid bigint unsigned, in i_d_pname varbinary(512),
  in i_s_name varbinary(512), in i_d_name varbinary(512))
begin
  declare aff_row int;
  declare s_name_len int;
  declare o_ret bigint;
  declare s_name_end varbinary(512);
  declare exit handler for sqlexception
  begin
    set o_ret = -14001;
    rollback;
    select o_ret;
  end;
  select 0 into aff_row;
  select -14000 into o_ret;
  set s_name_end = concat(i_s_name, char(255,255,255,255,255,255,255,255));
  select bit_length(i_s_name) / 8  into s_name_len;
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
      insert into t_meta_info
      (app_id, uid, pid, name, id, create_time, modify_time, size, ver_no, meta_info)
      select app_id, uid, i_d_pid | (1 << 63),
      concat(i_d_name, substring(name, s_name_len + 1, 8)),
      id, create_time, now(), size, ver_no, meta_info
      from t_meta_info
      where app_id = i_app_id and uid = i_uid and pid = i_s_pid | (1 << 63)
      and name >= i_s_name and name < s_name_end;
      delete from t_meta_info
      where app_id = i_app_id and uid = i_uid and pid = i_s_pid | (1 << 63)
      and name >= i_s_name and name < s_name_end;
      select row_count() into aff_row;
      if aff_row >= 1 then
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
