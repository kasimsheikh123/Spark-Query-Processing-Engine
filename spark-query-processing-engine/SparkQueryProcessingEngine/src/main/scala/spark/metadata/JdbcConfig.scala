package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the configuration for a JDBC data source.
  *
  * @param dbName          The name of the database.
  * @param dbUser          The username for connecting to the database.
  * @param dbPort          The port number for connecting to the database.
  * @param dbHost          The host address of the database.
  * @param dbPassword      The password for connecting to the database.
  * @param dbSchema        The schema of the database.
  * @param numPartitions   The number of partitions to be used while reading and writing dataframe.
  */
case class JdbcConfig(
    @JsonProperty("db_name") dbName: String,
    @JsonProperty("user_name") dbUser: String,
    @JsonProperty("port") dbPort: String,
    @JsonProperty("host") dbHost: String,
    @JsonProperty("password") dbPassword: String,
    @JsonProperty("db_schema") dbSchema: String,
    @JsonProperty("number_of_partitions") numPartitions: String
)
