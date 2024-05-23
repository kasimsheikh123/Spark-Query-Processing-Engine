Select count (*) from report.RE_INTRST_DVD_META_INFO
WHERE prcsng_status = 'U'
AND prcs_stage = 'FVC'
AND stmnt_type = '##STATEMENT_TYPE##'
AND sft_code = 'SFT-016'