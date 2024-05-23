with t1 as (
SELECT *,
    MAX(meta_info_seq_id) OVER (PARTITION BY orgnl_stmnt_id ORDER BY meta_info_seq_id DESC) AS max_meta_id
FROM validated_data_to_be_processed
)

SELECT
  t1.*,
  CASE
     WHEN pan_master.pan IS NULL THEN 'N'
     ELSE 'Y'
  END AS valid_pan_flag,
  CASE
     WHEN stmnt_type = 'NB' THEN 'A'
     WHEN stmnt_type = 'CB' AND max_meta_id = meta_info_seq_id THEN 'A'
     ELSE 'I'
  END AS status_flag
FROM
  t1
LEFT JOIN
  pan_master
ON
  t1.pan = pan_master.pan
