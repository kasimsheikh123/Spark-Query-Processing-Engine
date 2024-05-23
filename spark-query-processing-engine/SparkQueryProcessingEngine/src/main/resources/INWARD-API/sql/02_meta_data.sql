select l1.return_type,l2.*
FROM api_inward.gstn_returns_level_1_metadata l1
LEFT JOIN api_inward.gstn_returns_level_2_metadata l2 ON l1.gstn_l1_seq_id = l2.gstn_l1_seq_id
WHERE
  l2.process_status = '##TO_PROCESS_CODE##'
  AND (l1.req_date = ##DATE_TO_COMPARE## or 'null' = '##DATE_TO_COMPARE##')
  AND return_type = '##STATEMENT_TYPE##'
ORDER BY l2.updated_date
LIMIT ##MAX_FILES_PER_BATCH##