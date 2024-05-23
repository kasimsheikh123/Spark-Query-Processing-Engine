select distinct meta_info_seq_id,
SUBSTRING('##ERROR##',1,4000) as PRCS_ERR_DESC,
'DPF' as prcs_stage,
30 as ttl,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS CREATE_DATE,
'##DB_USER##' AS create_by,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS UPDATE_DATE,
'##DB_USER##' AS UPDATE_BY,
'##JOB_ID##' AS job_id
FROM ##SOURCE_VIEW##