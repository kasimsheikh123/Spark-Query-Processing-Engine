UPDATE report.re_intrst_dvd_meta_info a
SET
    total_rec_count = b.total_rec_count,
    total_amount = b.total_amount,
    total_vld_pan_rec_count = b.total_vld_pan_rec_count,
    total_vld_pan_amount = b.total_vld_pan_amount,
    upd_total_rec_count = b.upd_total_rec_count,
    upd_total_amount = b.upd_total_amount,
    upd_total_vld_pan_rec_count = b.upd_total_vld_pan_rec_count,
    upd_total_vld_pan_amount = b.upd_total_vld_pan_amount,
    prcs_date =  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP),
    prcsng_status = CASE
                        WHEN b.prcs_stage = 'DVF' THEN 'R'
                        WHEN b.prcs_stage = 'DPC' THEN 'A'
                        ELSE a.prcsng_status
                    END,
    prcs_stage = b.prcs_stage,
    update_date =  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP),
    update_by = '##DB_USER##',
    prcs_err_desc = b.prcs_err_desc
FROM (
     SELECT *
         FROM report.re_intrst_info_dtls_temp_spark
         WHERE job_id = '##JOB_ID##' AND prcs_stage != 'DMI'
         ORDER BY update_date DESC
        ) b
WHERE
    a.meta_info_seq_id = b.meta_info_seq_id
