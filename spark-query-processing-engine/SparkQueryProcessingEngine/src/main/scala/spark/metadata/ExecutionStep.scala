package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing a step in the execution process.
  *
  * @param executionName          The name of the execution step.
  * @param executionType          The type of the execution step.
  * @param dataSourceName         The name of the data source associated with the step.
  * @param dataSourceTarget       The target of the data source for the step.
  * @param dataSourceFileSchema   The file schema associated with the data source.
  * @param viewName               The name of the view associated with the step.
  * @param errorDataViewName      The name of the error data view associated with the step.
  * @param sqlFilePath            The file path for the SQL script used in the step.
  * @param dataPointName          The name of the data point associated with the step.
  * @param sourceView             The source view associated with the step.
  * @param targetColumn           The target column associated with the step.
  * @param typeOfQuery            The type of query used in the step.
  * @param positionalArguments    Array of positional arguments for the step.
  * @param toCache                Flag indicating whether to cache the result of the step.
  * @param exceptionHandling      The configuration for handling exceptions during the step.
  */
case class ExecutionStep(
    @JsonProperty("name") executionName: String,
    @JsonProperty("type") executionType: String,
    @JsonProperty("data_source_name") dataSourceName: String,
    @JsonProperty("data_source_target") dataSourceTarget: String,
    @JsonProperty("data_source_file_schema") dataSourceFileSchema: String,
    @JsonProperty("view_name") viewName: String,
    @JsonProperty("error_data_view_name") errorDataViewName: String,
    @JsonProperty("sql_file_path") sqlFilePath: String,
    @JsonProperty("data_point_name") dataPointName: String,
    @JsonProperty("source_view") sourceView: String,
    @JsonProperty("target_column") targetColumn: String,
    @JsonProperty("type_of_query") typeOfQuery: String,
    @JsonProperty("positional_arguments") positionalArguments: Array[String],
    @JsonProperty("cache") toCache: Boolean,
    @JsonProperty("exception_handling") exceptionHandling: ExceptionHandling
)
