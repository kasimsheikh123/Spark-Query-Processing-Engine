package spark.datasource

import org.apache.spark.sql.functions.{col, input_file_name, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.Logger
import spark.Util.buildSchema
import spark.metadata.{DataSourceConfig, FileSchemaConfig}

import scala.collection.mutable

/** Implementation of the DataSource trait for reading from file sources.
  *
  * @param dataSourceConfig        The configuration for the file data source.
  * @param spark                   The SparkSession for Spark-related operations.
  * @param logger                  The logger for logging informational messages.
  */
class FileSource(dataSourceConfig: DataSourceConfig)(implicit
    spark: SparkSession,
    logger: Logger
) extends DataSource {

  var fileSchemaConfig: Option[FileSchemaConfig] = None

  /** Reads data from file source based on the provided query or tableName and an optional schema.
   *
   * @param source     name of the file(s) to read.
   * @param fileSchema Optional schema to be applied to the read operation.
   * @return DataFrame containing the data based on the query or tableName.
   */
  override def loadDataFromSource(source: Any, fileSchema: FileSchemaConfig = null): DataFrame = {
    val fileConfig = dataSourceConfig.filePathConfig

    // build schema for file
    val schema = buildSchema(fileSchema.schema)
    // update fileSchemaConfig variable with current fileSchema
    fileSchemaConfig = Some(fileSchema)
    // Update schema if there's a specific column for corrupted records
    val updatedSchema =
      if (fileConfig.columnNameForCorruptedRecords != null || fileConfig.columnNameForCorruptedRecords.nonEmpty)
        schema.add(StructField(fileConfig.columnNameForCorruptedRecords, StringType, true))
      else schema

    // Base DataFrame for reading files
    val baseDF = spark.read
      .format(fileSchema.format)
      .options(makeOptions())

    // Read DataFrame based on tableName (either a single path or an array of paths)
    val dfWithSchema = source match {
      case arr: Array[String] =>
        val updatedPathArray = if (dataSourceConfig.filePathConfig.basePath != null) {
          arr.map(path => dataSourceConfig.filePathConfig.basePath + path)
        } else {
          arr
        }
        // check if paths exist and filter out only existing paths
        val filteredPaths = updatedPathArray.filter(f => new java.io.File(f).exists)
        logger.info(s"======PATHS NOT FOUND = ${updatedPathArray.diff(filteredPaths).mkString("\n")}======")
        if (updatedSchema != null) baseDF.schema(updatedSchema).load(filteredPaths: _*)
        else baseDF.load(filteredPaths: _*)

      case str: String =>
        if (updatedSchema != null) baseDF.schema(updatedSchema).load(str)
        else baseDF.load(str)
    }

    // Add SparkFilePathColumnName column to DataFrame
    val df = dfWithSchema.withColumn(
      fileConfig.SparkFilePathColumnName,
      regexp_replace(input_file_name(), "^file:/+", "/")
    )

    // If basePath is provided, adjust the SparkFilePathColumnName accordingly
    if (dataSourceConfig.filePathConfig.basePath != null) {
      val basePath = dataSourceConfig.filePathConfig.basePath
      df.withColumn(
        fileConfig.SparkFilePathColumnName,
        regexp_replace(col(fileConfig.SparkFilePathColumnName), basePath, "")
      )
    } else {
      df
    }
  }

  /** Generates options for configuring the file data source.
    *
    * @return A mutable Map representing the configuration options.
    */
  override def makeOptions(): mutable.Map[String, String] = {
    val configMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()
    val fileConfig = dataSourceConfig.filePathConfig
    val schemaConfig = fileSchemaConfig.get

    // create a config map from extra options string
    val SchemaConfigMap = schemaConfig.options.split(";").map(_.split("=")).map(a => (a(0).trim, a(1).trim)).toMap

    // Add options based on the file format
    if (schemaConfig.delimiter != null) configMap.put("delimiter", schemaConfig.delimiter)
    if (fileConfig.numPartitions != null) configMap.put("numPartitions", fileConfig.numPartitions)

    // Add option for the column name of corrupted records, if specified
    if (fileConfig.columnNameForCorruptedRecords != null || fileConfig.columnNameForCorruptedRecords.nonEmpty)
      configMap.put("columnNameOfCorruptRecord", fileConfig.columnNameForCorruptedRecords)

    logger.info(s"File source config details are - format - ${schemaConfig.format}")
    configMap ++ SchemaConfigMap
  }

  /** Not implemented for FileSource as it is a read-only data source.
   *
   * @param source The name of the table to read.
   * @return DataFrame containing the data from the specified table.
   */
  def loadQuery(source: String): DataFrame = ???

  /** Write given dataframe at the specified path
   *
   * @param target The name of the table to which the DataFrame should be written.
   * @param df     The DataFrame to be written to the table.
   */
  override def write(target: String, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .parquet(target)
  }

}
