with t1 as (
    select a.* from
    validated_cb_db_data a
    LEFT ANTI join
    unioned_error_data b
    on a.meta_info_seq_id = b.meta_info_seq_id
    WHERE a.meta_info_seq_id != a.target_table_meta_id
)
select
    meta_info_seq_id,
    'DMI' as prcs_stage,
    target_table_meta_id ,
    target_table_rsn,
    '##JOB_ID##' as job_id,
    cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS CREATE_DATE,
    '##DB_USER##' AS create_by,
    cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS UPDATE_DATE,
    '##DB_USER##' AS UPDATE_by
from t1