package spark.datasource

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.Logger
import spark.Constants._
import spark.metadata.{DataSourceConfig, FileSchemaConfig}

import scala.collection.mutable

/** Implementation of the DataSource trait for reading from and writing to Teradata databases.
  *
  * @param dataSourceConfig        The configuration for the Teradata data source.
  * @param spark                   The SparkSession for Spark-related operations.
  * @param logger                  The logger for logging informational messages.
  */
class TeraDataDataSource(dataSourceConfig: DataSourceConfig)(implicit
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
    val dbName = dataSourceConfig.jdbcConfig.dbName
    val tableNameWithSchema = s"$dbName.$tableNameCasted"
    spark.read
      .format("jdbc")
      .options(makeOptions())
      .option("dbtable", tableNameWithSchema)
      .load()
  }

  /** Generates options for configuring the Teradata data source.
    *
    * @return A mutable Map representing the configuration options.
    */
  def makeOptions(): mutable.Map[String, String] = {
    val configMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()

    val tdConfig = dataSourceConfig.jdbcConfig
    val dbHost = if (TD_DB_HOST.isEmpty) tdConfig.dbHost else TD_DB_HOST
    val dbPort = if (TD_DB_PORT.isEmpty) tdConfig.dbPort else TD_DB_PORT
    val dbName = if (TD_DB_NAME.isEmpty) tdConfig.dbName else TD_DB_NAME
    val dbUser = if (TD_DB_USER.isEmpty) tdConfig.dbUser else TD_DB_USER
    val dbPassword = if (TD_DB_PASSWORD.isEmpty) tdConfig.dbPassword else TD_DB_PASSWORD
    val numPartitions = if (TD_NUMBER_OF_PARTITIONS.isEmpty) tdConfig.numPartitions else TD_NUMBER_OF_PARTITIONS

    // Constructing the JDBC URL based on the provided configurations
    val url =
      if (dbPort == null || dbPort.isEmpty)
        s"jdbc:teradata://$dbHost/$dbName"
      else
        s"jdbc:teradata://$dbHost:$dbPort/$dbName"

    // Adding options to the configuration map
    configMap.put("url", url)
    configMap.put("user", dbUser)
    configMap.put("password", dbPassword)
    configMap.put("driver", "com.teradata.jdbc.TeraDriver")
    if (numPartitions != null) configMap.put("numPartitions", numPartitions)

    logger.info(s"DB config details are - url - $url, user - $dbUser, ")
    configMap
  }

  /** Reads data based on the provided query and an optional schema.
   * Returns the data as a DataFrame.
   *
   * @param source The query to execute.
   * @return DataFrame containing the data based on the query.
   */
  override def loadQuery(source: String): DataFrame = {
    spark.read
      .format("jdbc")
      .options(makeOptions())
      .option("query", source)
      .load()
  }

  /** Writes the given DataFrame `df` to the specified table `tableName`.
   *
   * @param tableName The name of the table to which the DataFrame should be written.
   * @param df        The DataFrame to be written to the table.
   */
  override def write(tableName: String, df: DataFrame): Unit = {
    val dbName = dataSourceConfig.jdbcConfig.dbName
    val tableNameWithSchema = s"$dbName.$tableName"
    df.write
      .format("jdbc")
      .options(makeOptions())
      .option("dbtable", tableNameWithSchema)
      .mode(SaveMode.Append)
      .save()
  }
}
