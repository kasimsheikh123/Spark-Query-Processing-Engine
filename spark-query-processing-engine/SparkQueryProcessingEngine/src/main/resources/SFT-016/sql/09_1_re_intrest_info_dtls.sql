SELECT
  dtls.meta_info_seq_id as target_table_meta_id,
  dtls.rsn as target_table_rsn ,
  dtls.fin_year_id as target_table_fin_year_id ,
  dtls.name as target_table_name ,
  dtls.pan as target_table_pan,
  dtls.valid_pan_flag as target_table_valid_pan_flag ,
  dtls.adhar_num as target_table_adhar_num ,
  dtls.mobile_no as target_table_mobile_no ,
  dtls.email_id as target_table_email_id ,
  dtls.acnt_num as target_table_acnt_num ,
  dtls.acnt_type as target_table_acnt_type ,
  dtls.intrst_amount as target_table_intrst_amount,
  dtls.status_flag
  from repoc_db.re_intrst_info_dtls dtls
where meta_info_seq_id in (##meta_seq_list##)