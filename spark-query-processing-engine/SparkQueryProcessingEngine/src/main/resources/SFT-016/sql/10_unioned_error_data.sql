with t1 as (select * from corrupted_data

union

select distinct
  meta_info_seq_id,
  CAST(isInvalidRecordReason AS INT) AS vldtn_id,
  COUNT(*) AS rec_count
from validation_failed_data
GROUP BY meta_info_seq_id, vldtn_id
)

select * ,
    cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS CREATE_DATE,
    '##DB_USER##' AS create_by
from t1
