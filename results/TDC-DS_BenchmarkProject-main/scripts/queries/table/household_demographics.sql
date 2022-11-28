drop table if exists household_demographics;
create table household_demographics (
  hd_demo_sk integer, hd_income_band_sk integer, 
  hd_buy_potential string, hd_dep_count integer, 
  hd_vehicle_count integer
) using parquet options ( path "${data_path}" )
