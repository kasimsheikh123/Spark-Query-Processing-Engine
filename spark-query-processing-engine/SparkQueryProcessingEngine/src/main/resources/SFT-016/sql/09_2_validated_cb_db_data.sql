with t1 as (
    select * from re_intrest_info_dtls a
    inner join
    original_meta_info_seq b
    on a.target_table_meta_id = b.current_meta_info_seq_id
),
t2 as (
    select * ,
    CONCAT_WS('|',
           target_table_rsn ,
           target_table_fin_year_id ,
           target_table_name ,
           target_table_pan,
           target_table_adhar_num ,
           target_table_mobile_no ,
           target_table_email_id ,
           target_table_acnt_num ,
           target_table_acnt_type ,
           target_table_intrst_amount) as target_concat,
    CONCAT_WS('|',
           `rsn`,
           `FY`,
           `Name`,
           `PAN`,
           `Aadhaar`,
           `Mobile`,
           `Email`,
           `Account Number`,
           `Type of Account`,
           `Interest`) as source_concat
    from validated_data a
    left join
    t1 b
    on (a.rsn = b.target_table_rsn) and (a.orgnl_stmnt_id = b.target_table_stmnt_id)
    where a.isInvalidRecord = 'false'
),

t3 as (
select * ,
CASE
    WHEN (target_stmnt_type = 'NB' and stmnt_type = 'DB' and source_concat != target_concat) THEN '51|'
    ELSE ''
END as db_validation,
CASE
    WHEN (stmnt_type != 'NB' and t2.target_table_meta_id IS NULL) THEN '49|'
    ELSE ''
END as rsn_not_present_validation
--CASE
--    WHEN (stmnt_type != 'NB' and status_flag = 'I') THEN '50|'
--    ELSE ''
--END as already_deleted_validation
from t2
),

t4 as (
select * from t3
where status_flag = 'A' AND status_flag IS NOT NULL
),

t5 as (
select t3.*,
 case
    when (t4.meta_info_seq_id is null AND t3.status_flag is not null) then '50|'
    else ''
 END as already_deleted_validation
 from t3
 left join t4
 on t3.rsn = t4.target_table_rsn and t3.orgnl_stmnt_id = t4.target_table_stmnt_id
)

select *,
 length(trim(concat(db_validation,rsn_not_present_validation,already_deleted_validation))) != 0 as is_invalid_data,
concat(db_validation,rsn_not_present_validation,already_deleted_validation) as invalid_data_error_desc
from t5
