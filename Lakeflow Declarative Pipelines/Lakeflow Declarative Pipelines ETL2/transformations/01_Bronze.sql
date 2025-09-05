create or refresh streaming table workspace.1_bronze_db.orders_bronze
as
select
*,
current_timestamp() as processing_time,
_metadata.file_name as source_file
from STREAM read_files(
  "/Volumes/workspace/ops/volumes" || "/orders",
  format => "json"
);