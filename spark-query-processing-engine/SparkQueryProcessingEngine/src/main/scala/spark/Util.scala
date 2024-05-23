package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.Logger
import spark.Constants.{PG_DB_TIMEZONE, TIMESTAMP_FORMAT}
import spark.datasource.DataSource
import spark.metadata.{ExecutionStep, FieldSchema, FileSchemaConfig, JobConfig}
import spark.methods.UserMethods.getUpdatedQuery
import spark.psql_operations.CurdOperation.performDmlOperation

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

/** Utility object containing various helper methods for the Spark Query Processing Engine.
  */
object Util {

  /** Reads the contents of a file from the resource path.
    *
    * @param sourcePath The path of the file within the resources.
    * @return The contents of the file as a string.
    * @throws Exception if the file does not exist.
    */
  def readFileFromResourcePath(sourcePath: String): String = {
    Try(Source.fromResource(sourcePath).mkString(""))
      .getOrElse(throw new Exception(s"$sourcePath does not exist. Please specify a valid path"))
  }

  /** Gets a mutable HashMap of DataSource configurations based on the provided JobConfig and shuffle partition count.
    *
    * @param jobConfig            The JobConfig object containing job-level configurations.
    * @param spark                The SparkSession.
    * @param logger               The logger for logging messages.
    * @return A mutable HashMap of DataSource configurations.
    */
  def getDataSourceConfigMap(
      jobConfig: JobConfig
  )(implicit spark: SparkSession, logger: Logger): mutable.HashMap[String, DataSource] = {
    val configMap = mutable.HashMap[String, DataSource]()
    jobConfig.dataSources.foreach(dataSourceConfig => {
      configMap.put(dataSourceConfig.name, datasource.DataProvider(dataSourceConfig))
    })
    configMap
  }

  /** Initializes a Spark session with specified configurations.
    *
    * @param appName              The name of the Spark application.
    * @param sparkConfigString    The spark Configuration String.
    * @return A SparkSession.
    */
  def getSparkSession(appName: String, sparkConfigString: String)(implicit logger: Logger): SparkSession = {
    // create config map from config string
    val configMap = sparkConfigString.split(";").map(_.split("=")).map(a => (a(0).trim, a(1).trim)).toMap
    // log spark config to be used
    logger.info(s"Spark config details are - ${configMap.mkString(" || ")}")
    // set spark config
    val conf = new SparkConf().setAppName(appName).setAll(configMap)
    // create spark session
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  }

  /** Builds a Spark DataFrame schema based on the provided array of FieldSchema.
    *
    * @param schemaObj The array of FieldSchema representing the schema.
    * @return A StructType representing the schema.
    */
  def buildSchema(schemaObj: Array[FieldSchema]): StructType = {
    val fields = schemaObj.map(field => {
      StructField(field.name, StringType, true)
    })
    StructType(fields)
  }

  /** Gets the current date and time formatted as per the specified timezone and timestamp format.
    *
    * @return A formatted string representing the current date and time.
    */
  def getDateTime: String = {
    val fromTimeZone = PG_DB_TIMEZONE
    val format = new SimpleDateFormat(TIMESTAMP_FORMAT)
    val date = new Date()
    format.setTimeZone(TimeZone.getTimeZone(fromTimeZone))
    format.format(date).toString
  }

  /** Gets a mutable HashMap of FileSchemaConfig based on the provided array of FileSchemaConfig.
    *
    * @param fileSchemaConfigs The array of FileSchemaConfig.
    * @return A mutable HashMap of FileSchemaConfig.
    */
  def getFileSchemaConfigMap(fileSchemaConfigs: Array[FileSchemaConfig]): mutable.HashMap[String, FileSchemaConfig] = {
    val configMap = mutable.HashMap[String, FileSchemaConfig]()
    fileSchemaConfigs.foreach(config => {
      configMap.put(config.fileSchemaName, config)
    })
    configMap
  }

  /** Checks if required fields are present in the provided ExecutionStep.
    *
    * @param executionStep The ExecutionStep to check.
    * @throws Exception if required fields are missing.
    */
  def checkRequiredFields(executionStep: ExecutionStep): Unit = {
    if (
      (executionStep.sourceView != null && executionStep.sqlFilePath != null)
      || (executionStep.sourceView == null && executionStep.sqlFilePath == null)
    ) throw new Exception(s"Please pass either `sql_file_path` or `source_view`")
  }

  /** Gets the Spark DataType based on the provided type name.
    *
    * @param typeName The name of the data type.
    * @return The corresponding Spark DataType.
    * @throws Exception if the type is not recognized.
    */
  def getTargetDataType(typeName: String): DataType = {
    val typeLower = if (typeName.toLowerCase.startsWith("decimal(")) "decimal" else typeName.toLowerCase

    val (a, b) = if (typeName.startsWith("decimal(")) {
      val digits = typeName.replace("decimal(", "").replace(")", "").split(",").map(_.trim.toInt)
      if (digits.length > 1) {
        (digits(0), digits(1))
      } else if (digits.length == 1) {
        (digits(0), 0)
      } else (10, 0)
    } else {
      (10, 0)
    }

    typeLower match {
      case "text"      => StringType
      case "string"    => StringType
      case "int"       => IntegerType
      case "integer"   => IntegerType
      case "long"      => LongType
      case "short"     => ShortType
      case "float"     => FloatType
      case "double"    => DoubleType
      case "boolean"   => BooleanType
      case "timestamp" => TimestampType
      case "date"      => DateType
      case "decimal"   => DecimalType(a, b)
      case _           => throw new Exception(s"Please add $typeLower in known DataTypes")
    }
  }

  /** Handles exceptions during query execution based on the provided ExecutionStep and exception details.
    *
    * @param executionStep      The ExecutionStep where the exception occurred.
    * @param maxFilesPerBatch   The maximum number of files per batch.
    * @param exception          The exception that occurred.
    * @param spark              The SparkSession.
    * @param logger             The logger for logging messages.
    * @param dataSourceMap      The mutable HashMap of DataSource configurations.
    * @param jobId              The job ID.
    */
  def handleException(
      executionStep: ExecutionStep,
      exception: Exception,
      maxFilesPerBatch: String = ""
  )(implicit
      spark: SparkSession,
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      jobId: String
  ): Unit = {

    // Check if exception handling configuration is provided in the ExecutionStep
    if (executionStep.exceptionHandling != null) {
      logger.info(s"====== PERFORMING EXCEPTION HANDLING ======")

      // Extract exception handling configuration
      val exceptionConfig = executionStep.exceptionHandling
      val dataSource: DataSource = dataSourceMap(exceptionConfig.dataSourceName)
      val dbUser: String = Try(dataSource.getConfig("user")).getOrElse("")
      val exceptionMessage = exception.getMessage.replace("'", "\"")
      val exceptionSourceView = if (exceptionConfig.sourceView != null) exceptionConfig.sourceView else ""
      val errorCode = if (exceptionConfig.errorCode != null) exceptionConfig.errorCode else ""

      // Read SQL file for exception handling
      val exceptionSqlString = readSqlFileFromPath(exceptionConfig.exceptionHandlingSqlPath)

      // Substitute placeholders in SQL file with actual values
      val updatedExceptionSqlString = getUpdatedQuery(
        exceptionSqlString,
        executionStep.positionalArguments,
        dbUser,
        maxFilesPerBatch,
        exceptionSourceView,
        exceptionMessage,
        errorCode
      )

      // Execute the updated SQL query
      val df = spark.sql(updatedExceptionSqlString)
      dataSource.write(exceptionConfig.targetTableName, df)

      // If an update metadata query is provided, execute it
      if (exceptionConfig.updateMetaDataQueryPath != null) {
        val sqlQuery = readSqlFileFromPath(exceptionConfig.updateMetaDataQueryPath)
        val updatedSqlQuery = getUpdatedQuery(
          sqlQuery,
          executionStep.positionalArguments,
          dbUser,
          maxFilesPerBatch,
          exceptionSourceView,
          exceptionMessage,
          errorCode
        )

        // Log the updated metadata update query
        logger.info(s"====== UPDATE QUERY FOR METADATA - $updatedSqlQuery ======")

        // Perform the update operation on metadata
        performDmlOperation(dataSource, "UPDATE", updatedSqlQuery)
      }
    }
  }

  /** Reads the contents of a SQL file from the provided path.
    *
    * @param sourcePath The path of the SQL file.
    * @return The contents of the SQL file as a string.
    */
  def readSqlFileFromPath(sourcePath: String): String = {
    Try(Source.fromResource(sourcePath).mkString).getOrElse(throw new Exception(s"PATH DOES NOT EXIST - $sourcePath"))
  }
}
