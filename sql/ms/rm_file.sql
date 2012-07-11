drop procedure rm_file;
delimiter $$
create procedure
rm_file(in i_app_id bigint, in i_uid bigint, in i_ppid bigint unsigned,
  in i_pid bigint unsigned, in i_pname varbinary(512), in i_name varbinary(512))
begin
  declare aff_row int;
  declare o_ret bigint;
  declare name_end varbinary(512);
  declare exit handler for sqlexception
  begin
    set o_ret = -14000;
    rollback;
    select o_ret;
  end;
  select 0 into aff_row;
  select -14000 into o_ret;
  set name_end = concat(i_name, char(255,255,255,255,255,255,255,255));
  start transaction;
  update t_meta_info set modify_time = now()
  where app_id = i_app_id and uid = i_uid and pid = i_ppid and name = i_pname;
  select count(1) into aff_row from t_meta_info
  where app_id = i_app_id and uid = i_uid and pid = i_ppid and name = i_pname;
  if aff_row = 1 then
    delete from t_meta_info
    where app_id = i_app_id and uid = i_uid and pid = i_pid | (1 << 63) and name >= i_name and name < name_end;
    select row_count() into aff_row;
    if aff_row >= 1 then
      set o_ret = 1;
    else
      set o_ret = -14001;
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
