select a.* from validated_data a
left join
unioned_error_data b
on a.meta_info_seq_id = b.meta_info_seq_id
where b.meta_info_seq_id IS NULL