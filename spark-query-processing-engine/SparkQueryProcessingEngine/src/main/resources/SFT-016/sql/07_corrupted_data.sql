select distinct
  meta_info_seq_id,
  CAST(12 AS INT) AS vldtn_id,
  COUNT(*) AS rec_count
FROM joined_meta_to_source
WHERE _corrupt_record IS NOT NULL
GROUP BY meta_info_seq_id, vldtn_id