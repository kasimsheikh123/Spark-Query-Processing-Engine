select distinct gstn_l2_seq_id,
file_path as file_path,
return_type as file_type,
2008 as process_status,
0 as nfs_counts,
'CSV schema error' as status_desc,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS created_date,
cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS updated_date,
'##JOB_ID##' AS spark_job_id
FROM joined_meta_to_source
WHERE _corrupt_record IS NOT NULL
AND csv_file_path is not null