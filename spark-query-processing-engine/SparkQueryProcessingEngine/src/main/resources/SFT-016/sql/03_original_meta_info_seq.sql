WITH t1 AS (
  SELECT DISTINCT COALESCE(orgnl_stmnt_id, stmnt_id) AS orgnl_stmnt_id
  FROM report.RE_INTRST_DVD_META_INFO ridmi
  WHERE prcsng_status = 'U' AND prcs_stage = 'FVC' AND sft_code = 'SFT-016' and stmnt_type = '##STATEMENT_TYPE##'
)

SELECT a1.meta_info_seq_id as current_meta_info_seq_id, COALESCE(a1.orgnl_stmnt_id, a1.stmnt_id) AS target_table_stmnt_id, a1.stmnt_type AS target_stmnt_type
FROM report.RE_INTRST_DVD_META_INFO a1
JOIN t1 a2 ON (a1.orgnl_stmnt_id = a2.orgnl_stmnt_id OR a1.stmnt_id = a2.orgnl_stmnt_id)
WHERE sft_code = 'SFT-016'