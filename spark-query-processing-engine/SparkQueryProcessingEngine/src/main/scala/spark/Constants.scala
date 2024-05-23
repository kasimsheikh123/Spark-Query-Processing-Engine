package spark

/**
 * Object containing constants used throughout the application.
 */
object Constants {

  // POSTGRES Environment Variable
  val DB_HOST: String = sys.env.getOrElse("PSQL_HOSTNAME", "")
  val DB_PORT: String = sys.env.getOrElse("PSQL_PORT", "")
  val DB_NAME: String = sys.env.getOrElse("PSQL_DBNAME", "")
  val DB_USER: String = sys.env.getOrElse("PSQL_USER", "")
  val DB_PASSWORD: String = sys.env.getOrElse("PSQL_PASSWORD", "")
  val DB_NUMBER_OF_PARTITIONS: String = sys.env.getOrElse("PSQL_NUMBER_OF_PARTITIONS", "")

  // TERADATA Environment Variable
  val TD_DB_HOST: String = sys.env.getOrElse("TERADATA_HOSTNAME", "")
  val TD_DB_PORT: String = sys.env.getOrElse("TERADATA_PORT", "")
  val TD_DB_NAME: String = sys.env.getOrElse("TERADATA_DBNAME", "")
  val TD_DB_USER: String = sys.env.getOrElse("TERADATA_USER", "")
  val TD_DB_PASSWORD: String = sys.env.getOrElse("TERADATA_PASSWORD", "")
  val TD_NUMBER_OF_PARTITIONS: String = sys.env.getOrElse("TERADATA_NUMBER_OF_PARTITIONS", "")

  // Data Source Constants
  val PG_DATASOURCE = "POSTGRES"
  val TD_DATASOURCE = "TERADATA"
  val FILE_DATASOURCE = "FILE"

  // Base Methods
  val READ_DATA_SOURCE = "READ_DATA_SOURCE"
  val RUN_QUERY_EXTRACT_DATA = "RUN_QUERY_EXTRACT_DATA"
  val MAKE_VIEW = "MAKE_VIEW"
  val RUN_DQL_QUERY = "RUN_DQL_QUERY"
  val RUN_DML_QUERY = "RUN_DML_QUERY"
  val WRITE_DATA_SOURCE = "WRITE_DATA_SOURCE"
  val PERFORM_DATA_VALIDATIONS = "PERFORM_DATA_VALIDATIONS"
  val FORMAT_COLUMNS_FOR_TARGET = "FORMAT_COLUMNS_FOR_TARGET"

  // User Defined Methods
  val UPDATE_TARGET_TABLE_TO_MARK_INVALID = "UPDATE_TARGET_TABLE_TO_MARK_INVALID"

  // Timezone and Timestamp Format Constants
  val PG_DB_TIMEZONE: String = sys.env.getOrElse("PG_DB_TIMEZONE", "Asia/Calcutta")
  val TIMESTAMP_FORMAT: String = sys.env.getOrElse("TIMESTAMP_FORMAT", "yyyy-MM-dd HH:mm:ss")
}
