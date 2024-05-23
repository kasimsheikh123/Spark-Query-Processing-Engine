package spark.datasource

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import spark.Constants.{FILE_DATASOURCE, PG_DATASOURCE, TD_DATASOURCE}
import spark.metadata.DataSourceConfig

object DataProvider {

  /** Creates an instance of a specific DataSource based on the provided DataSourceConfig.
    * The type of DataSource is determined by the configType property in the DataSourceConfig.
    *
    * @param dataSourceConfig        The configuration for the DataSource.
    * @param spark                   The SparkSession for Spark-related operations.
    * @param logger                  The logger for logging informational messages.
    * @return                        An instance of the specific DataSource implementation.
    */
  def apply(
      dataSourceConfig: DataSourceConfig
  )(implicit spark: SparkSession, logger: Logger): DataSource = {
    // Determine the type of DataSource based on the configType property in the DataSourceConfig
    dataSourceConfig.configType.toUpperCase match {
      case PG_DATASOURCE   => new PGDataSource(dataSourceConfig)
      case TD_DATASOURCE   => new TeraDataDataSource(dataSourceConfig)
      case FILE_DATASOURCE => new FileSource(dataSourceConfig)
      // Add more cases if additional DataSource types are introduced
    }
  }

}
