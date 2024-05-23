package spark.methods

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.Logger
import spark.Util._
import spark.datasource.DataSource
import spark.metadata.{ExecutionStep, FileSchemaConfig, PostLoadConfig}
import spark.methods.UserMethods.getUpdatedQuery
import spark.processor.ExecutionStepProcessor.{collectedListMap, dqlQueryResultListMap}
import spark.psql_operations.CurdOperation.{performDmlOperation, performDqlOperation}
import spark.validations.ValidationRules.makeValidationColumns

import scala.collection.mutable
import scala.util.Try

/** Object responsible for defining Base Methods.
  */
object BaseMethods {

  /** Reads data from a specified data source based on the provided [[ExecutionStep]].
    *
    * @param executionStep           The execution step containing information about the data source and how to read it.
    * @param maxFilesPerBatch        The maximum number of files to be processed in a single batch.
    * @param positionalArguments     Optional positional arguments for SQL queries.
    * @param logger                  The logger for logging informational messages.
    * @param dataSourceMap           A mutable HashMap containing data source configurations.
    * @param fileSchemaMap           A mutable HashMap containing file schema configurations.
    * @param jobId                   The identifier for the current job.
    * @param spark                   The SparkSession for Spark-related operations.
    */
  def readDataSource(
      executionStep: ExecutionStep,
      maxFilesPerBatch: String,
      positionalArguments: Array[String] = null
  )(implicit
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      jobId: String,
      spark: SparkSession
  ): Unit = {
    // Log information about the data source being read
    logger.info(
      s"====== reading data source : data_source_name - ${executionStep.dataSourceName} sql_file_path - ${executionStep.sqlFilePath} ======"
    )

    // Get the data source configuration
    val dataSource: DataSource = dataSourceMap(executionStep.dataSourceName)

    // Extract the database user from the data source configuration, defaulting to an empty string if not present
    val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")

    // If a file schema is specified,fetch the corresponding file schema configuration
    val fileSchemaObj = Try(fileSchemaMap(executionStep.dataSourceFileSchema)).getOrElse(null)

    // Choose the appropriate method to read data based on the data source type
    val resultedDF = executionStep.dataSourceName match {
      case "file_source" =>
        // For file sources, determine the target to read (path string to read) or data point name (path string list)
        val targetToRead =
          if (executionStep.dataPointName != null) collectedListMap(executionStep.dataPointName)
          else executionStep.dataSourceTarget
        dataSource.loadDataFromSource(targetToRead, fileSchemaObj)

      case _ =>
        // For other data sources, determine the target to read based on SQL file path or direct table name
        if (executionStep.sqlFilePath != null) {
          val sql = readSqlFileFromPath(executionStep.sqlFilePath)
          val targetToRead = getUpdatedQuery(sql, positionalArguments, dbUser, maxFilesPerBatch)
          dataSource.loadQuery(targetToRead)
        } else {
          val targetToRead = executionStep.dataSourceTarget
          dataSource.loadDataFromSource(targetToRead)
        }
    }

    // Create or replace a temporary view in Spark with the obtained DataFrame
    resultedDF.createOrReplaceTempView(executionStep.viewName)

    // Optionally cache the temporary view in Spark if specified in the execution step
    if (executionStep.toCache) spark.catalog.cacheTable(executionStep.viewName)
  }

  /** Runs a query to extract data based on the provided [[ExecutionStep]] and collects the result into a list.
    *
    * @param executionStep The execution step containing information about the query and target data.
    * @param spark         The SparkSession for Spark-related operations.
    * @param logger        The logger for logging informational messages.
    */
  def runQueryExtractData(
      executionStep: ExecutionStep
  )(implicit
      spark: SparkSession,
      logger: Logger
  ): Unit = {
    import spark.implicits._
    checkRequiredFields(executionStep)

    val df = if (executionStep.sourceView != null) {
      // If a source view is specified, use it directly
      spark.table(executionStep.sourceView)
    } else {
      // If a SQL file path is provided, read the SQL and execute it
      val sqlString = readSqlFileFromPath(executionStep.sqlFilePath)
      spark.sql(sqlString)
    }

    // Extract the target column data and collect it into a list
    val collectedList = df.select(col(executionStep.targetColumn)).as[String].collect()

    // Update the collected list in the global map for future reference
    collectedListMap.update(executionStep.dataPointName, collectedList)
  }

  /** Reads a SQL file from the provided file path, updates the query with positional arguments, executes the query,
    * and creates a temporary view in Spark. Optionally caches the view in Spark if specified in the execution step.
    *
    * @param executionStep The execution step containing information about the SQL file, positional arguments, and view details.
    * @param spark         The SparkSession for Spark-related operations.
    * @param logger        The logger for logging informational messages.
    * @param jobId         The identifier for the current job.
    */
  def makeView(executionStep: ExecutionStep)(implicit
      spark: SparkSession,
      logger: Logger,
      jobId: String
  ): Unit = {

    // Read the SQL file from the specified file path
    val sqlString = readSqlFileFromPath(executionStep.sqlFilePath)

    // Update the SQL query with positional arguments
    val updatedSqlString = getUpdatedQuery(sqlString, executionStep.positionalArguments)

    // Execute the SQL query and obtain a DataFrame
    val df = spark.sql(updatedSqlString)

    // Create or replace a temporary view in Spark with the obtained DataFrame
    df.createOrReplaceTempView(executionStep.viewName)

    // Optionally cache the temporary view in Spark if specified in the execution step
    if (executionStep.toCache) spark.catalog.cacheTable(executionStep.viewName)
  }

  /** Runs a DQL (Data Query Language) JDBC query using the provided [[ExecutionStep]].
    *
    * @param executionStep    The execution step containing information about the JDBC query, data source, and data collection.
    * @param maxFilesPerBatch The maximum number of files to be processed in a single batch.
    * @param logger           The logger for logging informational messages.
    * @param dataSourceMap    A mutable HashMap containing data source configurations.
    * @param jobId            The identifier for the current job.
    */
  def runDqlJdbcQuery(
      executionStep: ExecutionStep,
      maxFilesPerBatch: String
  )(implicit
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      jobId: String
  ): Unit = {
    // Read the SQL statement from the specified file path
    val sqlStatement = readSqlFileFromPath(executionStep.sqlFilePath)

    // Obtain the data source configuration
    val dataSource: DataSource = dataSourceMap(executionStep.dataSourceName)

    // Extract the database user from the data source configuration, defaulting to an empty string if not present
    val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")

    // Update the SQL statement with positional arguments and database user, considering maximum files per batch
    val updatedSqlPath = getUpdatedQuery(sqlStatement, executionStep.positionalArguments, dbUser, maxFilesPerBatch)

    // Perform the DQL operation using the data source and updated SQL statement
    val collectedList = performDqlOperation(dataSource, updatedSqlPath)

    // Update the collected DQL query result list in the global map for future reference
    dqlQueryResultListMap.update(executionStep.dataPointName, collectedList)
  }

  /** Runs a DML (Data Manipulation Language) JDBC query using the provided [[ExecutionStep]].
    *
    * @param executionStep     The execution step containing information about the JDBC query, data source, and query type.
    * @param maxFilesPerBatch  The maximum number of files to be processed in a single batch.
    * @param logger            The logger for logging informational messages.
    * @param dataSourceMap     A mutable HashMap containing data source configurations.
    * @param jobId             The identifier for the current job.
    */
  def runDmlJdbcQuery(
      executionStep: ExecutionStep,
      maxFilesPerBatch: String
  )(implicit
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      jobId: String
  ): Unit = {
    // Read the SQL statement from the specified file path
    val sqlStatement = readSqlFileFromPath(executionStep.sqlFilePath)

    // Obtain the data source configuration
    val dataSource: DataSource = dataSourceMap(executionStep.dataSourceName)

    // Extract the database user from the data source configuration, defaulting to an empty string if not present
    val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")

    // Update the SQL statement with positional arguments and database user, considering maximum files per batch
    val updatedSqlPath = getUpdatedQuery(sqlStatement, executionStep.positionalArguments, dbUser, maxFilesPerBatch)

    // Perform the DML operation using the data source, query type, and updated SQL statement
    performDmlOperation(dataSource, executionStep.typeOfQuery, updatedSqlPath)
  }

  /** Writes data to a specified data source based on the provided [[ExecutionStep]].
    *
    * @param executionStep        The execution step containing information about the data source and how to write to it.
    * @param maxFilesPerBatch     The maximum number of files to be processed in a single batch.
    * @param logger               The logger for logging informational messages.
    * @param spark                The SparkSession for Spark-related operations.
    * @param dataSourceMap        A mutable HashMap containing data source configurations.
    * @param jobId                The identifier for the current job.
    */
  def writeDataSource(
      executionStep: ExecutionStep,
      maxFilesPerBatch: String
  )(implicit
      logger: Logger,
      spark: SparkSession,
      dataSourceMap: mutable.HashMap[String, DataSource],
      jobId: String
  ): Unit = {
    // Obtain the data source configuration
    val dataSource = dataSourceMap(executionStep.dataSourceName)

    // Extract the database user from the data source configuration, defaulting to an empty string if not present
    val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")

    // Check if required fields in the execution step are populated
    checkRequiredFields(executionStep)

    // Determine the DataFrame to be written to the data source
    val df = if (executionStep.sourceView != null) {
      // If a source view is specified, use it directly
      spark.table(executionStep.sourceView)
    } else {
      // If a SQL file path is provided, read the SQL, update it, and execute the query to create a dataframe
      val sqlString = readSqlFileFromPath(executionStep.sqlFilePath)
      val updatedSqlPath = getUpdatedQuery(sqlString, executionStep.positionalArguments, dbUser, maxFilesPerBatch)
      logger.info(s"====== UPDATED SQL STRING : $updatedSqlPath ======")
      spark.sql(updatedSqlPath)
    }

    // Write the DataFrame to the specified target in the data source
    dataSource.write(executionStep.dataSourceTarget, df)
  }

  /** Calculates the number of batches required based on the provided [[PostLoadConfig]].
    *
    * @param postLoad          The post-load configuration specifying the data source, query, and batch parameters.
    * @param logger            The logger for logging informational messages.
    * @param dataSourceMap     A mutable HashMap containing data source configurations.
    * @param jobId             The identifier for the current job.
    * @return The calculated number of batches.
    */
  def calculateBatchCount(
      postLoad: PostLoadConfig
  )(implicit
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      jobId: String
  ): Int = {
    // Obtain the data source configuration
    val dataSource: DataSource = dataSourceMap(postLoad.dataSourceName)

    // Extract the database user from the data source configuration, defaulting to an empty string if not present
    val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")

    // Read the SQL statement from the specified file path
    val sqlStatement = readSqlFileFromPath(postLoad.batchQueryTarget)

    // Update the SQL statement with positional arguments and database user
    val updatedQuery = getUpdatedQuery(sqlStatement, postLoad.positionalArguments, dbUser)
    logger.info(s"======QUERY TO CALCULATE BATCH COUNT - $updatedQuery======")

    // Perform a DQL operation to get the record count based on the updated query
    val recordCount = performDqlOperation(dataSource, updatedQuery)

    // Convert maxFilesPerBatch to an integer
    val maxFilesPerBatch = postLoad.maxFilesPerBatch.toInt

    // Calculate the number of batches based on the record count and max files per batch
    val numOfBatches = if (maxFilesPerBatch < recordCount.head.head.toInt && recordCount.head.head.toInt != 0) {
      if (recordCount.head.head.toInt % maxFilesPerBatch == 0)
        recordCount.head.head.toInt / maxFilesPerBatch
      else
        (recordCount.head.head.toInt / maxFilesPerBatch) + 1
    } else if (recordCount.head.head.toInt == 0) {
      0
    } else {
      1
    }

    numOfBatches
  }

  /** Performs data validation on a DataFrame based on the provided [[ExecutionStep]].
    * Adds validation columns to the DataFrame, creates two new DataFrames - one for passed records and one for failed records,
    * and creates temporary views for further analysis.
    *
    * @param executionStep     The execution step containing information about the data source and how to validate it.
    * @param logger            The logger for logging informational messages.
    * @param fileSchemaMap     A mutable HashMap containing file schema configurations.
    * @param spark             The SparkSession for Spark-related operations.
    * @param jobId             The identifier for the current job.
    */
  def performDataValidation(
      executionStep: ExecutionStep
  )(implicit
      logger: Logger,
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      spark: SparkSession,
      jobId: String
  ): Unit = {
    // Check if required fields in the execution step are populated
    checkRequiredFields(executionStep)

    // Determine the DataFrame to be validated
    val df =
      if (executionStep.sourceView != null) spark.table(executionStep.sourceView)
      else {
        val sqlString = readSqlFileFromPath(executionStep.sqlFilePath)
        val updatedSqlPath =
          getUpdatedQuery(sqlString, executionStep.positionalArguments)
        spark.sql(updatedSqlPath)
      }

    // Obtain the file schema configuration for validation
    val fileSchemaObj = fileSchemaMap(executionStep.dataSourceFileSchema)

    // Create validation columns and split the DataFrame into passed and failed DataFrames
    val (passedDF, failedDF) = makeValidationColumns(fileSchemaObj, df)

    // Create or replace temporary views for further analysis
    passedDF.createOrReplaceTempView(executionStep.viewName)
    failedDF.createOrReplaceTempView(executionStep.errorDataViewName)
  }

  /** Formats columns in the source DataFrame based on the specified target data schema.
    * Casts columns to the corresponding data types and selects the desired columns for the target table.
    * Creates a temporary view for further processing.
    *
    * @param executionStep     The execution step containing information about the source data and target data schema.
    * @param logger            The logger for logging informational messages.
    * @param fileSchemaMap     A mutable HashMap containing file schema configurations.
    * @param spark             The SparkSession for Spark-related operations.
    */
  def formatColumnsForTarget(
      executionStep: ExecutionStep
  )(implicit
      logger: Logger,
      fileSchemaMap: mutable.HashMap[String, FileSchemaConfig],
      spark: SparkSession,
    jobId :String
  ): Unit = {
    // Obtain the file schema configuration for the target data
    val fileSchemaObj = fileSchemaMap(executionStep.dataSourceFileSchema)

    // Read the source data from the specified source view or sql path
    val sourceDF = if (executionStep.sourceView != null) spark.table(executionStep.sourceView)
    else {
      val sqlString = readSqlFileFromPath(executionStep.sqlFilePath)
      val updatedSqlPath =
        getUpdatedQuery(sqlString, executionStep.positionalArguments)
      spark.sql(updatedSqlPath)
    }

    // Format columns in the source DataFrame based on the target data schema
    val updatedDF = fileSchemaObj.schema.foldLeft(sourceDF)((sourceView, schema) => {
      sourceView.withColumn(
        schema.dbColumnName,
        col(schema.name).cast(getTargetDataType(schema.dataType.value.toString))
      )
    })

    // Select only the desired columns for the target table
    val selectedCols = fileSchemaObj.targetTableColumns.map(col)
    val selectedDF = updatedDF.select(selectedCols: _*)

    // Create or replace a temporary view for further processing
    selectedDF.createOrReplaceTempView(executionStep.viewName)
  }

}
