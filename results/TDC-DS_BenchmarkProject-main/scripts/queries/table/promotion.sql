drop table if exists promotion;
create table promotion (
  p_promo_sk integer, 
  p_promo_id string, 
  p_start_date_sk integer, 
  p_end_date_sk integer, 
  p_item_sk integer, 
  p_cost decimal(15, 2), 
  p_response_target integer, 
  p_promo_name string, 
  p_channel_dmail string, 
  p_channel_email string, 
  p_channel_catalog string, 
  p_channel_tv string, 
  p_channel_radio string, 
  p_channel_press string, 
  p_channel_event string, 
  p_channel_demo string, 
  p_channel_details string, 
  p_purpose string, 
  p_discount_active string
) using parquet options ( path "${data_path}" )
