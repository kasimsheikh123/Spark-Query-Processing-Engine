package spark.datasource

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.Logger
import spark.Constants._
import spark.metadata.{DataSourceConfig, FileSchemaConfig}

import scala.collection.mutable

/** Implementation of the DataSource trait for reading from and writing to PostgreSQL databases.
  *
  * @param dataSourceConfig        The configuration for the PostgreSQL data source.
  * @param spark                   The SparkSession for Spark-related operations.
  * @param logger                  The logger for logging informational messages.
  */
class PGDataSource(dataSourceConfig: DataSourceConfig)(implicit
    spark: SparkSession,
    logger: Logger
) extends DataSource {

  /** Reads the entire table specified by `tableName` and returns the data as a DataFrame.
   *
   * @param source     The name of the table to read.
   * @param fileSchema Optional schema to be applied to the read operation.
   * @return DataFrame containing the data from the specified table.
   */
  override def loadDataFromSource(source: Any, fileSchema: FileSchemaConfig = null): DataFrame = {
    val tableNameCasted = source.asInstanceOf[String]
    logger.info(s"=== PSQL Read TableName: - $tableNameCasted ===== ")
    spark.read
      .format("jdbc")
      .options(makeOptions())
      .option("dbtable", tableNameCasted)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  /** Reads data based on the provided query and an optional schema.
   * Returns the data as a DataFrame.
   *
   * @param tableName The query to execute.
   * @return DataFrame containing the data based on the query.
   */
  override def loadQuery(tableName: String): DataFrame = {

    logger.info(s"=== PSQL Query: - $tableName ====")
    spark.read
      .format("jdbc")
      .options(makeOptions())
      .option("query", tableName)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  /** Writes the given DataFrame `df` to the specified table `tableName`.
   *
   * @param target The name of the table to which the DataFrame should be written.
   * @param df     The DataFrame to be written to the table.
   */
  override def write(target: String, df: DataFrame): Unit = {
    logger.info(s"=== PSQL Write TableName: - $target ===")
    df.write
      .format("jdbc")
      .options(makeOptions())
      .option("dbtable", target)
      .mode(SaveMode.Append)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  /** Generates options for configuring the PostgreSQL data source.
    *
    * @return A mutable Map representing the configuration options.
    */
  def makeOptions(): mutable.Map[String, String] = {
    val configMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()

    val pgConfig = dataSourceConfig.jdbcConfig
    val dbHost = if (DB_HOST.isEmpty) pgConfig.dbHost else DB_HOST
    val dbPort = if (DB_PORT.isEmpty) pgConfig.dbPort else DB_PORT
    val dbName = if (DB_NAME.isEmpty) pgConfig.dbName else DB_NAME
    val dbUser = if (DB_USER.isEmpty) pgConfig.dbUser else DB_USER
    val dbPassword = if (DB_PASSWORD.isEmpty) pgConfig.dbPassword else DB_PASSWORD
    val numPartitions = if (DB_NUMBER_OF_PARTITIONS.isEmpty) pgConfig.numPartitions else DB_NUMBER_OF_PARTITIONS

    // Constructing the JDBC URL based on the provided configurations
    val url =
      if (dbPort == null || dbPort.isEmpty)
        s"jdbc:postgresql://$dbHost/$dbName"
      else
        s"jdbc:postgresql://$dbHost:$dbPort/$dbName"

    // Adding options to the configuration map
    configMap.put("url", url)
    configMap.put("user", dbUser)
    configMap.put("password", dbPassword)
    if (numPartitions != null) configMap.put("numPartitions", numPartitions)

    logger.info(s"DB config details are - url - $url, user - $dbUser, ")
    configMap
  }
}
