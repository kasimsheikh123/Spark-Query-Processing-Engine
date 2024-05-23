package spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.Util.{getDataSourceConfigMap, getFileSchemaConfigMap, getSparkSession}
import spark.datasource.DataSource
import spark.metadata.{FileSchemaConfig, JobConfig}
import spark.parser.{FileSchemaConfigParser, JobConfigParser}
import spark.processor.ExecutionStepProcessor

import scala.collection.mutable
import scala.util.Try

/** Object containing the entry point for the Spark Query Processing Engine.
  */
object QueryProcessingEngine {

  /** Main method that serves as the entry point for the Spark Query Processing Engine.
    *
    * @param args Command-line arguments passed to the engine.
    */
  def main(args: Array[String]): Unit = {
    // Create an instance of the QueryProcessingEngine with the provided command-line arguments
    val engineInstance = new QueryProcessingEngine(args)

    // Invoke the process method to initiate the query processing
    engineInstance.process()
  }
}

/** Class representing a Spark Query Processing Engine.
  *
  * @param args Command-line arguments passed to the engine.
  */
class QueryProcessingEngine(args: Array[String]) {
  implicit val logger: Logger = LoggerFactory.getLogger("QueryProcessingEngine")

  /** Processes Spark queries based on the provided command-line arguments.
    * Initializes Spark session, configures job parameters, and executes the query processing steps.
    */
  def process(): Unit = {
    logger.info("================ SPARK QUERY PROCESSING ENGINE V1 ==================")

    /*
       Get the spark configuration string from command-line arguments (default: spark.sql.shuffle.partitions=5)
       The format for this program argument is "key1=value1;key2=value2;key3=value3"
       example : "spark.driver.extraJavaOptions=-Xss4M;spark.sql.shuffle.partitions=5;spark.serializer=org.apache.spark.serializer.KryoSerializer;spark.master=local[*]"
    */
    val sparkConfigString: String = Try(args(0)).getOrElse("spark.sql.shuffle.partitions=5").trim

    // Get the job name from command-line arguments (default: SFT-016)
    val jobName: String = Try(args(1)).getOrElse("SFT-016").trim
    logger.info(s"======JOB NAME - $jobName======")
    // Generate a unique job ID using the current timestamp
    implicit val jobId: String = System.currentTimeMillis().toString.trim

    // Paths to job configuration and file schema files based on SFT type name
    val jobConfigPath = s"$jobName/job_config.json"
    val fileSchemaPath = s"$jobName/file_schema.json"

    // Parse job configuration and file schema
    implicit val jobConfigObj: JobConfig = JobConfigParser(jobConfigPath)
    implicit val fileSchemaConfigObj: Array[FileSchemaConfig] = FileSchemaConfigParser(fileSchemaPath)

    // Initialize Spark session
    implicit val spark: SparkSession = getSparkSession("QueryProcessingEngine",sparkConfigString)

    // Initialize data source configurations and file schema configurations
    implicit val dataSourceMap: mutable.HashMap[String, DataSource] =
      getDataSourceConfigMap(jobConfigObj)
    implicit val fileSchemaConfigMap: mutable.HashMap[String, FileSchemaConfig] =
      getFileSchemaConfigMap(fileSchemaConfigObj)

    // Execute the Query Processing Steps
    ExecutionStepProcessor()
  }

}
