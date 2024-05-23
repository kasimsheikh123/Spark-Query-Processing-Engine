select
meta_info_seq_id,
'DVF' as prcs_stage,
'DATA VERIFICATION FAILED' as PRCS_ERR_DESC,
30 as ttl,
'##JOB_ID##' as job_id,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS CREATE_DATE,
'##DB_USER##' AS create_by
from unioned_error_data