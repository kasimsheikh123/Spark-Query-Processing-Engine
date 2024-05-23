with t1 as (
    select *
    FROM joined_meta_to_source
    WHERE csv_file_path IS NULL OR _corrupt_record IS NOT NULL
)

select * from joined_meta_to_source a
LEFT ANTI join
t1
on a.gstn_l2_seq_id = t1.gstn_l2_seq_id