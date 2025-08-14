create or refresh  streaming table 2_silver_db.orders_silver as
select
order_id,
timestamp(ordertimestamp) as order_timestamp,
customer_id,
source_file
from STREAM 1_bronze_db.orders_bronze;