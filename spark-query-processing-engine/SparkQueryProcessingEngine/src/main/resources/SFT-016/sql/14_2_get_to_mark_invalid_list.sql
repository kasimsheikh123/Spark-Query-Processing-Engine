select
 target_table_meta_id ,
    target_table_rsn
 from report.re_intrst_info_dtls_temp_spark
 where job_id = '##JOB_ID##' and prcs_stage = 'DMI'