select distinct gstn_l2_seq_id,
file_path as file_path,
return_type as file_type,
##ERROR_CODE## as process_status,
0 as nfs_counts,
SUBSTRING('##ERROR##',1,200) as status_desc,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS created_date,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS updated_date,
'##JOB_ID##' AS spark_job_id
FROM ##SOURCE_VIEW##