package spark.processor

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import spark.Constants._
import spark.Util.handleException
import spark.datasource.DataSource
import spark.metadata.{ExecutionStep, FileSchemaConfig, JobConfig}
import spark.methods.BaseMethods._
import spark.methods.UserMethods.updateTargetTableToMarkInvalid

import scala.collection.mutable
import scala.util.control.Breaks

/** Object responsible for processing execution steps defined in a Spark job configuration.
  * It performs both pre-load and post-load steps.
  */
object ExecutionStepProcessor {

  // Maps to store intermediate results and DQL query results
  val collectedListMap: mutable.HashMap[String, Array[String]] = mutable.HashMap[String, Array[String]]()
  val dqlQueryResultListMap: mutable.HashMap[String, List[List[String]]] = mutable.HashMap[String, List[List[String]]]()

  /** Entry point to process execution steps.
    *
    * @param jobConfigObj    Job configuration object.
    * @param spark           SparkSession.
    * @param logger          Logger.
    * @param dataSourceMap   Map of data sources.
    * @param fileSchemaMap   Map of file schema configurations.
    * @param jobId           Job ID.
    */
  def apply()(implicit
      jobConfigObj: JobConfig,
      spark: SparkSession,
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      jobId: String
  ): Unit = {
    // Log the job ID for identification
    logger.info(s"====== JOB ID - $jobId ======")
    // Perform pre-load steps
    if (jobConfigObj.preLoadSteps != null) performPreLoadSteps()
    // Perform post-load steps
    if (jobConfigObj.postLoadSteps != null) performPostLoadSteps()
  }

  /** Performs the pre-load steps specified in the job configuration.
    *
    * @param jobConfigObj  Job configuration object.
    * @param spark         SparkSession.
    * @param logger        Logger.
    * @param dataSourceMap Map of data sources.
    * @param fileSchemaMap Map of file schema configurations.
    * @param jobId         Job ID.
    */
  def performPreLoadSteps()(implicit
      jobConfigObj: JobConfig,
      spark: SparkSession,
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      jobId: String
  ): Unit = {

    // Use Breaks to allow breaking out of the loop on encountering an exception
    val loop = new Breaks
    loop.breakable {
      jobConfigObj.preLoadSteps.foreach(executionStep => {
        // Log information about the current step
        logger.info(s"======Performing Step - ${executionStep.executionName}======")
        try {
          // Perform the execution step
          performExecutionStep(executionStep)
        } catch {
          // Handle exceptions gracefully
          case exception: Exception =>
            exception.printStackTrace()
            // Handle the exception and break out of the loop
            handleException(executionStep, exception)
            loop.break()
        }
        // Log completion of the step
        logger.info(s"======Done Performing Step - ${executionStep.executionName}======")
      })
    }
  }

  /** Performs post-load steps of the job, iterating through each post-load configuration.
    *
    * @param jobConfigObj  Job configuration object.
    * @param spark         SparkSession.
    * @param logger        Logger.
    * @param dataSourceMap Map of data sources.
    * @param fileSchemaMap Map of file schema configurations.
    * @param jobId         Job ID.
    */
  def performPostLoadSteps()(implicit
      jobConfigObj: JobConfig,
      spark: SparkSession,
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      jobId: String
  ): Unit = {
    // Iterate through each post-load configuration
    jobConfigObj.postLoadSteps.foreach(postLoad => {
      logger.info(
        s"=====PROCESSING EXECUTION STEPS WITH PROGRAM ARGUMENTS : ${postLoad.positionalArguments.mkString(",")}======"
      )
      val maxFilesPerBatch: String = postLoad.maxFilesPerBatch
      val positionalArguments: Array[String] = postLoad.positionalArguments
      val batchCount = if (postLoad.isBatchJob) calculateBatchCount(postLoad) else 1

      // Iterate through each batch
      for (batchNo <- 1 to batchCount) {
        logger.info(s"======Processing For Batch - $batchNo======")
        // Use Breaks to allow breaking out of the loop on encountering an exception
        val loop = new Breaks
        loop.breakable {
          // Iterate through each execution step in the post-load configuration
          postLoad.executionStep.foreach(executionStep => {
            logger.info(s"======Performing Step - ${executionStep.executionName}======")
            try {
              // Perform the execution step for the current batch
              performExecutionStep(executionStep, maxFilesPerBatch, positionalArguments)
            } catch {
              case exception: Exception =>
                exception.printStackTrace()
                // Handle exception and break out of the loop
                handleException(executionStep, exception, maxFilesPerBatch)
                loop.break()
            }
            logger.info(s"======Done Performing Step - ${executionStep.executionName}======")
          })
        }
        logger.info(s"======Processing For Batch - $batchNo Finished======")
      }
    })
  }

  /** Performs the specified execution step.
    *
    * @param executionStep           Execution step to be performed.
    * @param maxFilesPerBatch        Maximum number of files per batch.
    * @param positionalArguments     Array of positional arguments.
    * @param spark                   SparkSession.
    * @param logger                  Logger.
    * @param dataSourceMap           Map of data sources.
    * @param fileSchemaMap           Map of file schema configurations.
    * @param jobId                   Job ID.
    */
  def performExecutionStep(
      executionStep: ExecutionStep,
      maxFilesPerBatch: String = "",
      positionalArguments: Array[String] = null
  )(implicit
      spark: SparkSession,
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      jobId: String
  ): Unit = {
    // Perform execution steps based on the execution type
    executionStep.executionType.toUpperCase match {
      case READ_DATA_SOURCE                    => readDataSource(executionStep, maxFilesPerBatch, positionalArguments)
      case RUN_QUERY_EXTRACT_DATA              => runQueryExtractData(executionStep)
      case MAKE_VIEW                           => makeView(executionStep)
      case RUN_DQL_QUERY                       => runDqlJdbcQuery(executionStep, maxFilesPerBatch)
      case RUN_DML_QUERY                       => runDmlJdbcQuery(executionStep, maxFilesPerBatch)
      case WRITE_DATA_SOURCE                   => writeDataSource(executionStep, maxFilesPerBatch)
      case PERFORM_DATA_VALIDATIONS            => performDataValidation(executionStep)
      case FORMAT_COLUMNS_FOR_TARGET           => formatColumnsForTarget(executionStep)
      case UPDATE_TARGET_TABLE_TO_MARK_INVALID => updateTargetTableToMarkInvalid(executionStep)
      // add more methods here if needed
    }
  }

}
