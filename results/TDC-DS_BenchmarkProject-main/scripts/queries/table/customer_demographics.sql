drop table if exists customer_demographics;
create table customer_demographics (
  cd_demo_sk integer, cd_gender string, 
  cd_marital_status string, cd_education_status string, 
  cd_purchase_estimate integer, cd_credit_rating string, 
  cd_dep_count integer, cd_dep_employed_count integer, 
  cd_dep_college_count integer
) using parquet options ( path "${data_path}" )