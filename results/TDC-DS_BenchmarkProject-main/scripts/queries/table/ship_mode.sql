drop table if exists ship_mode;
create table ship_mode (
  sm_ship_mode_sk integer, sm_ship_mode_id string, 
  sm_type string, sm_code string, sm_carrier string, 
  sm_contract string
) using parquet options ( path "${data_path}" )