create or refresh materialized view 3_gold_db.orders_by_date_vw
as
select
  date(order_timestamp) as order_date,
  count(*) total_daily_orders
from 2_silver_db.orders_silver
group by date(order_timestamp)