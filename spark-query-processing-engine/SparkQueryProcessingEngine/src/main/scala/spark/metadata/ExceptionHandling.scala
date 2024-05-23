package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for handling exceptions during query execution.
  *
  * @param errorCode                 The error code associated with the exception.
  * @param targetTableName           The target table name for writing exception data.
  * @param exceptionHandlingSqlPath  The file path for the SQL script used in exception handling.
  * @param updateMetaDataQueryPath   The file path for the SQL script used to update metadata after exception handling.
  * @param sourceView                The source view associated with the exception (if applicable).
  * @param dataSourceName            The name of the data source associated with the exception.
  */
case class ExceptionHandling(
    @JsonProperty("error_code") errorCode: String,
    @JsonProperty("target_table_name") targetTableName: String,
    @JsonProperty("exception_handling_sql_path") exceptionHandlingSqlPath: String,
    @JsonProperty("update_metadata_query_path") updateMetaDataQueryPath: String,
    @JsonProperty("source_view") sourceView: String,
    @JsonProperty("data_source_name") dataSourceName: String
)
