package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for a Spark job.
  *
  * @param jobName        The name of the Spark job.
  * @param dataSources    Array of data source configurations for the job.
  * @param preLoadSteps   Array of pre-load execution steps for the job.
  * @param postLoadSteps  Array of post-load configuration steps for the job.
  */
case class JobConfig(
    @JsonProperty("job_name") jobName: String,
    @JsonProperty("datasources") dataSources: Array[DataSourceConfig],
    @JsonProperty("pre_load_steps") preLoadSteps: Array[ExecutionStep],
    @JsonProperty("post_load_steps") postLoadSteps: Array[PostLoadConfig]
)
