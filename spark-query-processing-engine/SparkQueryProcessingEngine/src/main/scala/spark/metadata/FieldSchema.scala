package spark.metadata

import com.fasterxml.jackson.annotation.JsonProperty

/** Case class representing the schema definition for a field.
  *
  * @param name               The name of the field.
  * @param dbColumnName       The database column name associated with the field.
  * @param dataType           Metadata information for the data type of the field.
  * @param nullable           Metadata information for whether the field is nullable.
  * @param maxLength          Metadata information for the maximum length of the field.
  * @param minLength          Metadata information for the minimum length of the field.
  * @param isNotExponential   Metadata information for whether the field allows exponential notation.
  * @param equalColumn        Metadata information for a field equal to another column.
  * @param valueList          Metadata information for a field's allowed values list.
  */
case class FieldSchema(
    @JsonProperty("name") name: String,
    @JsonProperty("db_column_name") dbColumnName: String,
    @JsonProperty("dataType") dataType: ColumnMetaData,
    @JsonProperty("nullable") nullable: ColumnMetaData,
    @JsonProperty("max_length") maxLength: ColumnMetaData,
    @JsonProperty("min_length") minLength: ColumnMetaData,
    @JsonProperty("is_not_exponential") isNotExponential: ColumnMetaData,
    @JsonProperty("equal_column") equalColumn: ColumnMetaData,
    @JsonProperty("values_list") valueList: ColumnMetaData
)
