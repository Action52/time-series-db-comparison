drop table if exists income_band;
create table income_band (
  ib_income_band_sk integer, ib_lower_bound integer, 
  ib_upper_bound integer
) using parquet options ( path "${data_path}" )
