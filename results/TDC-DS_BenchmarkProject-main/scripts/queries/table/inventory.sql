drop table if exists inventory;
create table inventory (
  inv_date_sk integer, inv_item_sk integer, 
  inv_warehouse_sk integer, inv_quantity_on_hand integer
) using parquet options ( path "${data_path}" )