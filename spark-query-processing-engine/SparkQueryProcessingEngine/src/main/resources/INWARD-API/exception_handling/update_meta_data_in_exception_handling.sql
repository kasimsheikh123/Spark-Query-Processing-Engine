with t1 as (
	select gstn_l2_seq_id,
	COALESCE(MAX(CAST(td_counts AS INTEGER)), 0) AS td_counts,
	max(cast(process_status as integer)) as process_status
	from dep.gstn_returns_level_2_metadata_spark
	where spark_job_id = '##JOB_ID##'
	group by gstn_l2_seq_id
)

 update dep.gstn_returns_level_2_metadata nm set
 process_status = t1.process_status,
 td_counts = t1.td_counts,
 updated_date = cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)
 from t1 where nm.gstn_l2_seq_id = t1.gstn_l2_seq_id