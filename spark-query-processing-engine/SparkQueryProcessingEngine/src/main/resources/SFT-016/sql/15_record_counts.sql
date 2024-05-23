SELECT
    meta_info_seq_id,
    COUNT(1) AS total_rec_count,
    SUM(intrst_amount) AS total_amount,
    SUM(CASE WHEN valid_pan_flag = 'Y' THEN 1 ELSE 0 END) AS total_vld_pan_rec_count,
    SUM(CASE WHEN valid_pan_flag = 'Y' THEN intrst_amount ELSE 0 END) AS total_vld_pan_amount,
    SUM(CASE WHEN status_flag = 'A' THEN 1 ELSE 0 END) AS upd_total_rec_count,
    SUM(CASE WHEN status_flag = 'A' THEN intrst_amount ELSE 0 END) AS upd_total_amount,
    SUM(CASE WHEN status_flag = 'A' AND valid_pan_flag = 'Y' THEN 1 ELSE 0 END) AS upd_total_vld_pan_rec_count,
    SUM(CASE WHEN status_flag = 'A' AND valid_pan_flag = 'Y' THEN intrst_amount ELSE 0 END) AS upd_total_vld_pan_amount,
     cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS CREATE_DATE,
    'postgres' AS create_by,
    cast('##CURRENT_TIMESTAMP##' as TIMESTAMP) AS UPDATE_DATE,
    'postgres' AS UPDATE_by,
    'DPC' as prcs_stage,
    30 as ttl,
    '##JOB_ID##' as job_id
FROM
    repoc_db.re_intrst_info_dtls
where meta_info_seq_id in (##meta_seq_list##)
group by meta_info_seq_id