select * from meta_data
left join
input_data
on input_data.csv_file_path = meta_data.file_path