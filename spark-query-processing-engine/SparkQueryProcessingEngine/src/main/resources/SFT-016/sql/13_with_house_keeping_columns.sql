select * ,
  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS CREATE_DATE,
  '##DB_USER##' AS create_by,
  cast('##CURRENT_TIMESTAMP##' as TIMESTAMP)  AS UPDATE_DATE,
  '##DB_USER##' AS UPDATE_by
from selected_columns