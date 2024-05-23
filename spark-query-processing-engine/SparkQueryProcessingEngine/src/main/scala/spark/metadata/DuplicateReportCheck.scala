package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for checking duplicate reports.
  *
  * @param typeOfUniqueness   The type of uniqueness for duplicate check.
  * @param orderBy            The column used for ordering in duplicate check.
  * @param uniqueColumnsList  The list of columns considered for uniqueness check.
  * @param errorCode          The error code to be associated with duplicate records.
  */
case class DuplicateReportCheck(
    @JsonProperty("type_of_uniqueness") typeOfUniqueness: String,
    @JsonProperty("order_by_column") orderBy: String,
    @JsonProperty("unique_check_columns") uniqueColumnsList: Array[String],
    @JsonProperty("error_code") errorCode: String
)
