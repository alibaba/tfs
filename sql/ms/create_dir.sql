drop procedure create_dir;
delimiter $$
create procedure
create_dir(in i_app_id bigint, in i_uid bigint, in i_ppid bigint unsigned,
      in i_pname varbinary(512), in i_pid bigint unsigned, in i_id bigint,
      in i_name varbinary(512))
begin
    declare aff_row int;
    declare o_ret bigint;
    declare exit handler for sqlexception
    begin
        set o_ret = -14001;
        rollback;
        select o_ret;
    end;
    select 0 into aff_row;
    select -14000 into o_ret;
    start transaction;
    if i_pid = 0 then
      insert into t_meta_info (app_id, uid, pid, name, id, create_time, modify_time, size)
      values (i_app_id, i_uid, 0, i_name, i_id, now(), now(), 0);
      select row_count() into aff_row;
    else
      update t_meta_info set modify_time = now()
      where app_id = i_app_id and uid = i_uid and pid = i_ppid and name = i_pname;
      select count(1) into aff_row from t_meta_info
      where app_id = i_app_id and uid = i_uid and pid = i_ppid and name = i_pname;
      if aff_row = 1 then
        insert into t_meta_info (app_id, uid, pid, name, id, create_time, modify_time, size)
        values (i_app_id, i_uid, i_pid, i_name, i_id, now(), now(), 0);
        select row_count() into aff_row;
      end if;
    end if;
    set o_ret = aff_row;
    if o_ret <= 0 then
        set o_ret = -14002;
        rollback;
    else
        commit;
    end if;
    select o_ret;
end $$
delimiter ;
