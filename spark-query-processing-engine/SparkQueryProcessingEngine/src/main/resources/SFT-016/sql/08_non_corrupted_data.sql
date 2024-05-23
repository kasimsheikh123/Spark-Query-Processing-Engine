--SELECT *
--FROM joined_meta_to_source
--WHERE csv_file_path IS Not NULL
--AND _corrupt_record is Null ;
select * from joined_meta_to_source a
LEFT ANTI join
corrupted_data b
on a.meta_info_seq_id = b.meta_info_seq_id
WHERE a.csv_file_path IS Not NULL
AND a._corrupt_record is Null