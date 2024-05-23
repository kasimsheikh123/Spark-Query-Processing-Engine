package spark.validations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}
import spark.metadata.FileSchemaConfig

import scala.collection.mutable.ListBuffer

/** Object containing validation rules for a DataFrame based on a given file schema configuration.
 */
object ValidationRules {

  /** Generates validation columns for each field in the DataFrame based on the provided file schema configuration.
   *
   * @param fileSchemaConfig The configuration specifying the file schema.
   * @param df               The DataFrame to be validated.
   * @return A tuple containing the validated DataFrame and a DataFrame containing failed validation records.
   */
  def makeValidationColumns(fileSchemaConfig: FileSchemaConfig, df: DataFrame): (DataFrame, DataFrame) = {
    // ListBuffer to store validation conditions for each field
    val conditionCols: ListBuffer[(String, Column)] = new ListBuffer[(String, Column)]()

    // Iterate through each field definition in the schema
    fileSchemaConfig.schema.foreach(fieldDef => {
      val conditionColName = "inValid_" + fieldDef.name

      // Validation condition for integer type
      val onlyIntExp: Column =
        if (fieldDef.dataType != null && fieldDef.dataType.value.toString == "integer")
          when(
            length(regexp_replace(translate(trim(col(fieldDef.name)), "0123456789", "@@@@@@@@@@"), "@", "")) > 0,
            s"${fieldDef.dataType.errorCode}|"
          )
            .otherwise("")
        else
          lit("")
      conditionCols.append((s"${conditionColName}_onlyIntExp", onlyIntExp))

      // Validation condition for maximum length
      val maxExp: Column =
        if (fieldDef.maxLength != null)
          when(
            length(col(fieldDef.name)) > lit(fieldDef.maxLength.value.toString.toLong),
            s"${fieldDef.maxLength.errorCode}|"
          )
            .otherwise(lit(""))
        else
          lit("")
      conditionCols.append((s"${conditionColName}_maxLength", maxExp))

      // Validation condition for minimum length
      val minExp: Column =
        if (fieldDef.minLength != null)
          when(length(col(fieldDef.name)) === lit(fieldDef.minLength.value.toString.toInt), "")
            .otherwise(lit(s"${fieldDef.minLength.errorCode}|"))
        else
          lit("")
      conditionCols.append((s"${conditionColName}_minLength", minExp))

      // Validation condition for nullable fields
      val nullableExp: Column =
        if (fieldDef.nullable != null && fieldDef.nullable.value.toString == "false")
          when(col(fieldDef.name).isNull, s"${fieldDef.nullable.errorCode}|").otherwise("")
        else
          lit("")
      conditionCols.append((s"${conditionColName}_nullable", nullableExp))

      // Validation condition for non-exponential fields
      val exponentialExp: Column =
        if (fieldDef.isNotExponential != null && fieldDef.isNotExponential.value.toString == "true")
          when(
            col(fieldDef.name).rlike("^[+-]?\\d+(\\.\\d+)?[eE][+-]?\\d+$"),
            s"${fieldDef.isNotExponential.errorCode}|"
          )
            .otherwise("")
        else
          lit("")
      conditionCols.append((s"${conditionColName}_exponentialExp", exponentialExp))

      // Validation condition for equality with another column
      val fyYearExp: Column =
        if (fieldDef.equalColumn != null && fieldDef.equalColumn.value.toString.nonEmpty)
          when(col(fieldDef.name) === col(fieldDef.equalColumn.value.toString) || col(fieldDef.name).isNull, "")
            .otherwise(s"${fieldDef.equalColumn.errorCode}|")
        else
          lit("")
      conditionCols.append((s"${conditionColName}_fyYearExp", fyYearExp))

      // Validation condition for values in a predefined list
      val valueListExp: Column =
        if (fieldDef.valueList != null)
          when(trim(col(fieldDef.name)).isin(fieldDef.valueList.value.asInstanceOf[List[String]]: _*), "")
            .otherwise(s"${fieldDef.valueList.errorCode}|")
        else
          lit("")
      conditionCols.append((s"${conditionColName}_valueListExp", valueListExp))
    })

    // Combine all individual validation conditions into a single column
    val validClauseExpr: Column = concat(conditionCols.toList.map(_._2): _*)

    // Mark records with validation reasons
    val validationMarkedDF = df
      .withColumn("isInvalidRecordReason", functions.trim(validClauseExpr))

    // Check for duplicate records if specified in the schema configuration
    val validatedDF = if (fileSchemaConfig.duplicateReportsCheck != null) {
      // Concatenate columns to check for duplicates
      val concatColumns =
        fileSchemaConfig.duplicateReportsCheck.map(f => coalesce(col(s"invalid_${f.typeOfUniqueness}"), lit("")))
      val invalidMarkedDF = fileSchemaConfig.duplicateReportsCheck.foldLeft(validationMarkedDF)((df, field) => {
        // Extract unique columns for duplicate check
        val cols = field.uniqueColumnsList.map(c => col(c))
        // Define a window specification for ordering and partitioning
        val uniqueColWindow = Window
          .partitionBy(col(s"${field.typeOfUniqueness}_concat_cols"), col(field.orderBy))
          .orderBy(field.orderBy)
        // Apply window functions for checking duplicates
        df
          .withColumn(s"${field.typeOfUniqueness}_concat_cols", concat_ws("|", cols: _*))
          .withColumn(field.typeOfUniqueness, row_number.over(uniqueColWindow))
          .withColumn(
            s"invalid_${field.typeOfUniqueness}",
            when(col(field.typeOfUniqueness) > lit("1"), s"${field.errorCode}|").otherwise("")
          )
      })
      // Combine duplicate validation information with other validation reasons
      invalidMarkedDF
        .withColumn("duplicateIsInvalidRecordReason", concat(concatColumns: _*))
        .withColumn(
          "isInvalidRecordReason",
          concat(col("isInvalidRecordReason"), col("duplicateIsInvalidRecordReason"))
        )
        .withColumn("isInvalidRecord", length(functions.trim(col("isInvalidRecordReason"))) =!= lit(0))

    } else {
      // Only consider standard validation conditions
      validationMarkedDF.withColumn("isInvalidRecord", length(functions.trim(col("isInvalidRecordReason"))) =!= lit(0))
    }

    // Extract failed validation records
    val failedValidationRecordsDF = validatedDF
      .filter(validatedDF.col("isInvalidRecord") === true)
      .withColumn("isInvalidRecordReason", explode(split(col("isInvalidRecordReason"), "[|]")))
      .filter(col("isInvalidRecordReason") =!= "")

    // Return the validated DataFrame and the DataFrame containing failed validation records
    (validatedDF, failedValidationRecordsDF)
  }
}
