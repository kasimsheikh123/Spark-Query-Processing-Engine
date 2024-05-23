package spark.methods

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import spark.Util.getDateTime
import spark.datasource.DataSource
import spark.metadata.ExecutionStep
import spark.processor.ExecutionStepProcessor.{collectedListMap, dqlQueryResultListMap}
import spark.psql_operations.CurdOperation.performUpdateBatchOperation

import scala.collection.mutable

/** Object responsible for defining User Methods.
 */
object UserMethods {

  /** Updates the target table to mark records as invalid based on the provided data points.
    * Performs batch updates to set the status_flag to 'I' for specified records in the target table.
    *
    * @param executionStep    The execution step containing information about the data source and how to update the target table.
    * @param logger           The logger for logging informational messages.
    * @param dataSourceMap    A mutable HashMap containing data source configurations.
    * @param spark            The SparkSession for Spark-related operations.
    * @param jobId            The identifier for the current job.
    */
  def updateTargetTableToMarkInvalid(
      executionStep: ExecutionStep
  )(implicit
      logger: Logger,
      dataSourceMap: mutable.HashMap[String, DataSource],
      spark: SparkSession,
      jobId: String
  ): Unit = {

    // Obtain the data source configuration
    val dataSource = dataSourceMap(executionStep.dataSourceName)

    // Retrieve the list of data points from the global map
    val dataList = dqlQueryResultListMap(executionStep.dataPointName)

    // Specify the target table name with schema
    val tableNameWithSchema = "repoc_db.re_intrst_info_dtls"

    // Generate update queries based on the data points to mark records as invalid
    val updateQueries = dataList
      .map(row => {
        val metaId = row(0)
        val rsnNumber = row(1)
        s"UPDATE $tableNameWithSchema SET status_flag = 'I' WHERE meta_info_seq_id = '$metaId' AND rsn = '$rsnNumber';"
      })
      .distinct
      .toArray

    // Perform batch updates to the target table
    performUpdateBatchOperation(dataSource, executionStep.typeOfQuery, updateQueries)
  }

  /** Updates a SQL script with dynamic values, such as positional arguments, user, timestamp, etc.
    *
    * @param sqlScript              The original SQL script to be updated.
    * @param positionalArguments    An array of positional arguments to replace in the SQL script.
    * @param dbUser                 The database user to replace in the SQL script.
    * @param maxFilesPerBatch       The maximum number of files per batch to replace in the SQL script.
    * @param sourceView             The source view to replace in the SQL script.
    * @param exception              The exception message to replace in the SQL script.
    * @param errorCode              The error code to replace in the SQL script.
    * @param jobId                  The identifier for the current job.
    * @param logger                 The logger for logging informational messages.
    * @return The updated SQL script.
    */
  def getUpdatedQuery(
      sqlScript: String,
      positionalArguments: Array[String] = null,
      dbUser: String = "",
      maxFilesPerBatch: String = "",
      sourceView: String = "",
      exception: String = "",
      errorCode: String = ""
  )(implicit jobId: String, logger: Logger): String = {
    // Replace positional arguments in the SQL script
    val updatedSqlStatement = if (positionalArguments != null) {
      positionalArguments.zipWithIndex.foldLeft(sqlScript)((sqlScript, argument) => {
        if (argument != null && argument._2 == 0) {
          sqlScript.replace("##STATEMENT_TYPE##", argument._1)
        }
        else if(argument != null && argument._2 == 1){
          sqlScript.replace("##TO_PROCESS_CODE##", argument._1)
        }
        else if (argument != null && argument._2 == 2) {
          sqlScript.replace("##DATE_TO_COMPARE##", argument._1)
        }
        else {
          sqlScript
        }
      })
    } else {
      sqlScript
    }

    // Replace collected list values in the SQL script
    val sqlString = collectedListMap.foldLeft(updatedSqlStatement) { case (sqlString, (key, value)) =>
      val valueArray = value.map(item => s"'$item'")
      sqlString.replace(s"##${key}##", valueArray.mkString(","))
    }

    // Replace additional dynamic values in the SQL script
    val updatedString = sqlString
      .replace("##MAX_FILES_PER_BATCH##", maxFilesPerBatch)
      .replace("##DB_USER##", dbUser)
      .replace("##JOB_ID##", jobId)
      .replace("##CURRENT_TIMESTAMP##", getDateTime)
      .replace("##SOURCE_VIEW##", sourceView)
      .replace("##ERROR##", exception)
      .replace("##ERROR_CODE##", errorCode)

    updatedString
  }

}
