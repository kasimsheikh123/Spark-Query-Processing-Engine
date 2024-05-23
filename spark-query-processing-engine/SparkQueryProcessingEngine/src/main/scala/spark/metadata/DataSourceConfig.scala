package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for a data source.
  *
  * @param configType     The type of the data source configuration.
  * @param name           The name of the data source.
  * @param jdbcConfig     The JDBC configuration for the data source.
  * @param filePathConfig The file path configuration for the data source.
  */
case class DataSourceConfig(
    @JsonProperty("type") configType: String,
    @JsonProperty("name") name: String,
    @JsonProperty("jdbc_config") jdbcConfig: JdbcConfig,
    @JsonProperty("file_path_config") filePathConfig: FileSourceConfig
)
