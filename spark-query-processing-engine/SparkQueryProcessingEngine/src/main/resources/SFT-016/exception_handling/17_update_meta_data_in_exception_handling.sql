UPDATE report.re_intrst_dvd_meta_info AS a
SET
    prcs_err_desc = b.prcs_err_desc,
    prcs_stage = b.prcs_stage,
    update_date = cast('##CURRENT_TIMESTAMP##' as TIMESTAMP),
    update_by = '##DB_USER##',
    prcs_date =  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)
FROM (
    SELECT DISTINCT meta_info_seq_id, prcs_err_desc, prcs_stage
    FROM report.re_intrst_info_dtls_temp_spark
    WHERE prcs_stage = 'DPF'
    AND job_id = '##JOB_ID##'
) AS b
WHERE a.meta_info_seq_id = b.meta_info_seq_id