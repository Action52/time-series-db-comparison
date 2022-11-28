drop table if exists customer;
create table customer (
  c_customer_sk integer, c_customer_id string, 
  c_current_cdemo_sk integer, c_current_hdemo_sk integer, 
  c_current_addr_sk integer, c_first_shipto_date_sk integer, 
  c_first_sales_date_sk integer, c_salutation string, 
  c_first_name string, c_last_name string, 
  c_preferred_cust_flag string, c_birth_day integer, 
  c_birth_month integer, c_birth_year integer, 
  c_birth_country string, c_login string, 
  c_email_address string, c_last_review_date_sk integer
) using parquet options ( path "${data_path}" )
