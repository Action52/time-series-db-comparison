drop table if exists dbgen_version;
create table dbgen_version
(
    dv_version                string                  ,
    dv_create_date            date                          ,
    dv_create_time            timestamp                          ,
    dv_cmdline_args           string                 
) using parquet options ( path "${data_path}" )