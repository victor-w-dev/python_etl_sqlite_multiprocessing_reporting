with T1 as (select count(*) as total_no_from_area from area)--/(select count(*) from areacnty))
,T2 as (select count(*) as total_no_from_areacnty from areacnty)
select cast(total_no_from_area as float)/cast(total_no_from_areacnty as float)*100 from T1, T2