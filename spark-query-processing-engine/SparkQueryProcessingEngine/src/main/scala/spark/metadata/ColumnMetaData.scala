package spark.metadata
import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing metadata information for a column.
  *
  * @param value     The value of the column.
  * @param errorCode The error code associated with the column.
  */
case class ColumnMetaData(
    @JsonProperty("value") value: Any,
    @JsonProperty("error_code") errorCode: String
)
