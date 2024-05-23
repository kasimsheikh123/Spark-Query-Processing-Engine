package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for a file data source.
  *
  * @param basePath                       The base path for the file source.
  * @param columnNameForCorruptedRecords  The column name for storing corrupted records.
  * @param SparkFilePathColumnName        The column name for storing Spark file paths.
  */
case class FileSourceConfig(
    @JsonProperty("base_path") basePath: String,
    @JsonProperty("column_name_for_corrupted_records") columnNameForCorruptedRecords: String,
    @JsonProperty("spark_file_path_column_name") SparkFilePathColumnName: String,
    @JsonProperty("number_of_partitions") numPartitions: String
)
