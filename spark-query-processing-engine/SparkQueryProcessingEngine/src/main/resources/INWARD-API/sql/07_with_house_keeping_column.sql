select * ,
  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS insert_date_api
from selected_columns