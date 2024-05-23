package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for a file schema.
  *
  * @param fileSchemaName        The name of the file schema.
  * @param schema                Array of field schemas defining the structure of the file.
  * @param delimiter             The delimiter used in the file.
  * @param format                The format of the file (e.g., "csv", "parquet").
  * @param options               A string containing options to apply while reading (e.g., "mode=PERMISSIVE;nullValue=//N").
  * @param duplicateReportsCheck Array of configurations for checking duplicate reports.
  * @param targetTableColumns    Array of column names for the target table associated with the file schema.
  */
case class FileSchemaConfig(
    @JsonProperty("name") fileSchemaName: String,
    @JsonProperty("delimiter") delimiter: String,
    @JsonProperty("format") format: String,
    @JsonProperty("extra_options") options: String,
    @JsonProperty("schema") schema: Array[FieldSchema],
    @JsonProperty("duplicate_reports_check") duplicateReportsCheck: Array[DuplicateReportCheck],
    @JsonProperty("target_table_columns") targetTableColumns: Array[String]
)
