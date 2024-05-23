package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for post-load operations in a Spark job.
  *
  * @param isBatchJob            Flag indicating whether the post-load operation is a batch job.
  * @param maxFilesPerBatch      Maximum number of files per batch for post-load operation.
  * @param batchQueryTarget      Target for the batch query in post-load operation.
  * @param positionalArguments   Array of positional arguments for the post-load operation.
  * @param executionStep         Array of execution steps for the post-load operation.
  * @param dataSourceName        The name of the data source associated with the post-load operation.
  */
case class PostLoadConfig(
    @JsonProperty("is_batch_job") isBatchJob: Boolean,
    @JsonProperty("max_files_per_batch") maxFilesPerBatch: String,
    @JsonProperty("batch_query_target") batchQueryTarget: String,
    @JsonProperty("positional_arguments") positionalArguments: Array[String],
    @JsonProperty("execution_steps") executionStep: Array[ExecutionStep],
    @JsonProperty("data_source_name") dataSourceName: String
)
