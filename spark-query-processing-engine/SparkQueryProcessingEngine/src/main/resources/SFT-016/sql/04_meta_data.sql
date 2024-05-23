SELECT *
FROM report.RE_INTRST_DVD_META_INFO
WHERE prcsng_status = 'U'
AND prcs_stage = 'FVC'
AND sft_code = 'SFT-016'
AND stmnt_type = '##STATEMENT_TYPE##'
ORDER BY update_date
LIMIT ##MAX_FILES_PER_BATCH##