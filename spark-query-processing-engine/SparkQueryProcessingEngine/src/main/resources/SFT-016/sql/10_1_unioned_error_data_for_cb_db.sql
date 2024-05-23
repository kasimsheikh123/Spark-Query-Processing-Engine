with t1 as(
select meta_info_seq_id ,
 explode(split(invalid_data_error_desc, "[|]")) as exploded_invalid_data_error_desc
 from validated_cb_db_data
 where is_invalid_data = 'true'
),

t2 as (
select distinct
         meta_info_seq_id,
         CAST(exploded_invalid_data_error_desc AS INT) AS vldtn_id,
         COUNT(*) AS rec_count
from t1
WHERE exploded_invalid_data_error_desc != ''
GROUP BY meta_info_seq_id, vldtn_id

union

select * from corrupted_data

union

select distinct
  meta_info_seq_id,
  CAST(isInvalidRecordReason AS INT) AS vldtn_id,
  COUNT(*) AS rec_count
from validation_failed_data
GROUP BY meta_info_seq_id, vldtn_id
)

select * ,
  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS CREATE_DATE,
  '##DB_USER##' AS create_by
from t2
