drop table if exists time_dim;
create table time_dim (
  t_time_sk integer, t_time_id string, 
  t_time integer, t_hour integer, t_minute integer, 
  t_second integer, t_am_pm string, 
  t_shift string, t_sub_shift string, 
  t_meal_time string
) using parquet options ( path "${data_path}" )