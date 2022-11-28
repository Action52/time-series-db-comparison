drop table if exists reason;
create table reason (
  r_reason_sk integer,
  r_reason_id string,
  r_reason_desc string
)
using parquet options ( path "${data_path}" )