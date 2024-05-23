# SPARK QUERY PROCESSING ENGINE

The **SPARK QUERY PROCESSING ENGINE** is an Apache Spark program written in Scala designed to efficiently process queries using Spark's distributed computing capabilities.

## Table of Contents

- [SPARK QUERY PROCESSING ENGINE](#spark-query-processing-engine)
  - [Introduction](#introduction)
  - [Program Arguments](#program-arguments)
  - [Project Structure](#project-structure)

## Introduction

The **SPARK QUERY PROCESSING ENGINE** is built on Apache Spark and written in Scala to provide a powerful platform for processing queries at scale. It leverages Spark's distributed computing capabilities to handle large datasets efficiently.

## Program Arguments

The program accepts command-line arguments to customize its behavior. Two key arguments are:

- ### Spark Configuration String

  - **Description:** This is a Spark Configuration String that will be used to get configurations for Spark Session.
  - **Default:** "spark.sql.shuffle.partitions=5"
  - **Format:** "key1=value1;key2=value2;key3=value3"
  - **Example:** "spark.driver.extraJavaOptions=-Xss4M;spark.sql.shuffle.partitions=5;spark.master=local[*]"

- ### Job Name
  - **Description:** This String determines what Job to process.
  - **Default:** "SFT-016"

## Project Structure
- The project structure includes the following directory and file layout:
```
SparkQueryProcessingEngine
│
└── src
    └── main
        └── resources
            └── job-name (Specify job name to be used while accepting the job name through program argument)
                ├── files_schema.json
                ├── job_config.json
                ├── sql
                │   └── (SQL files for the main processing)
                └── exception_handling
                    └── (SQL file for exception handling)
```
  - ### file_schema.json
```
[
  {
    "name": "example-data-set",
    "format": "csv",
    "delimiter": ",",
    "extra_options": "header=true;multiline=true;ignoreLeadingWhiteSpace=true;ignoreTrailingWhiteSpace=true",
    "schema": [
      {
        "name": "User ID",
        "db_column_name": "user_id",
        "dataType": {
          "value": "string",
          "error_code": ""
        },
        "nullable": {
          "value": "false",
          "error_code": "101"
        },
        "max_length": {
          "value": "20",
          "error_code": "102"
        },
        "values_list": {
          "value": ["A123", "B456", "C789", "D012"],
          "error_code": "103"
        },
        "is_not_exponential": {
          "value": "true",
          "error_code": "104"
        },
        "min_length": {
          "value": "4",
          "error_code": "105"
        }
      },
      // Add more schema elements as needed
    ],
    "duplicate_reports_check": [
      {
        "type_of_uniqueness": "unique_user_check",
        "order_by_column": "registration_date",
        "unique_check_columns": ["user_id", "registration_date"],
        "error_code": "201"
      },
      // Add more duplicate reports check elements as needed
    ],
    "target_table_columns": [
      "user_id",
      "registration_date",
      // Add more target table columns as needed
    ]
  },
  // Add more configurations as needed
]
```

  - ### file_schema.json Configurations

      #### FileSchemaConfig

      | Key | Type | Description |
      | --- | ---- | ----------- |
      | name | String | The name of the file schema. |
      | delimiter | String | The delimiter used in the file. |
      | format | String | The format of the file. |
      | extra_options | String | Extra options for processing the file. Format: "key1=value1;key2=value2;key3=value3" |
      | Schema Configuration | Array[[`FieldSchema`](#fieldschema)] | An array representing the schema of the file. |
      | Duplicate Reports Check Configuration | Array[[`DuplicateReportCheck`](#duplicatereportcheck)] | An array representing checks for duplicate reports. |
      | target_table_columns | Array[Syring] | An array representing columns in the target table. |
      
      #### FieldSchema
          
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | name | String | The name of the field. |
      | db_column_name | String | The name of the corresponding database column. |
      | dataType | [`ColumnMetaData`](#columnmetadata) | Metadata for the data type of the field. |
      | nullable | [`ColumnMetaData`](#columnmetadata) | Metadata for whether the field is nullable. |
      | max_length | [`ColumnMetaData`](#columnmetadata) | Metadata for the maximum length of the field. |
      | min_length | [`ColumnMetaData`](#columnmetadata) | Metadata for the minimum length of the field. |
      | is_not_exponential | [`ColumnMetaData`](#columnmetadata) | Metadata for whether the field should not be in exponential format. |
      | equal_column | [`ColumnMetaData`](#columnmetadata) | Metadata for an equal column. |
      | values_list | [`ColumnMetaData`](#columnmetadata) | Metadata for a list of values. |
      
      #### DuplicateReportCheck
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | type_of_uniqueness | String | Specifies the type of uniqueness check. |
      | order_by_column | String | The column by which the reports are ordered for the check. |
      | unique_check_columns | Array[String] | An array representing columns for uniqueness check. |
      | error_code | String | The error code associated with the uniqueness check. |
      
      #### ColumnMetaData
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | value | Any | The value of the property. |
      | error_code | String | The error code associated with the property. |
    
  - ### job_congig.json
  - ### job_config.json Configurations

      #### JobConfig
    
      | Key | Type | Description |
      | --- | ---- | ----------- |
      | job_name | String | The name of the job. |
      | datasources | Array[[`DataSourceConfig`](#datasourceconfig)] | An array representing data sources for the job. |
      | pre_load_steps | Array[[`ExecutionStep`](#executionstep)] | An array representing pre-load execution steps. |
      | post_load_steps | Array[[`PostLoadConfig`](#postloadconfig)] | An array representing post-load configuration steps. |
      
      #### DataSourceConfig
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | type | String | The type of the data source configuration. Either `file`, `postgres` or `teradata` |
      | name | String | The name of the data source. |
      | jdbc_config | [`JdbcConfig`](#jdbcconfig) | JDBC configuration for the data source. if type is `postgres` or `teradata` |
      | file_path_config | [`FileSourceConfig`](#filesourceconfig) | File source configuration for the data source. if type is `file` |
      
      #### JdbcConfig
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | db_name | String | The name of the database. |
      | user_name | String | The username for database access. |
      | port | String | The port for database access. |
      | host | String | The host address for database access. |
      | password | String | The password for database access. |
      | number_of_partitions | String | The number of partitions. |
      
      #### FileSourceConfig
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | base_path | String | The base path for the file source. |
      | column_name_for_corrupted_records | String | The column name for corrupted records. |
      | spark_file_path_column_name | String | The column name for Spark file path. |
      | number_of_partitions | String | The number of partitions. |

      #### PostLoadConfig
      
      | Key | Type | Description |
      | --- | ---- | ----------- |
      | is_batch_job | Boolean | Indicates if it's a batch job. |
      | max_files_per_batch | String | The maximum number of files per batch (if it's a batch job) |
      | data_source_name | String | The name of the data source (if it's a batch job), specified in `name` of [`DataSourceConfig`](#datasourceconfig) |
      | batch_query_target | String | The sql query path for the batch query (if it's a batch job) |
      | positional_arguments | Array[String] | An array representing positional arguments. |
      | execution_steps | Array[[`ExecutionStep`](#executionstep)] | An array representing execution steps for post-load configuration. |
      
    
      #### ExecutionStep
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | name | String | The name of the execution step. |
      | type | String | The type of execution (`READ_DATA_SOURCE`,`RUN_QUERY_EXTRACT_DATA`,`MAKE_VIEW`,`RUN_DQL_QUERY`,`RUN_DML_QUERY`,`WRITE_DATA_SOURCE`,`PERFORM_DATA_VALIDATIONS`,`FORMAT_COLUMNS_FOR_TARGET`,`UPDATE_TARGET_TABLE_TO_MARK_INVALID`) |
      | data_source_name | String | The name of the data source. specified in `name` of [`DataSourceConfig`](#datasourceconfig) |
      | data_source_target | String | The table name or path where data will be written in case of `WRITE_DATA_SOURCE` |
      | data_source_file_schema | String | The file schema to use. specified in `name` of [`FieldSchema`](#fieldschema) |
      | view_name | String | The view name that will be used to create a view for resulted dataframe |
      | error_data_view_name | String | The view name for error data. in case of `PERFORM_DATA_VALIDATIONS`|
      | sql_file_path | String | The file path for SQL query. this will be used to create a dataframe |
      | data_point_name | String | The name of the data point. the name for saving the intermediate result for `RUN_QUERY_EXTRACT_DATA` or to retrive the saved data |
      | source_view | String | The source view. if view is already created |
      | target_column | String | The target column which has to be collected in case of `RUN_QUERY_EXTRACT_DATA` |
      | type_of_query | String | The type of query (UPDATE,CREATE,DELETE) in case of `RUN_DML_QUERY` and `UPDATE_TARGET_TABLE_TO_MARK_INVALID` |
      | cache | Boolean | Indicates whether to cache the dataframe. |
      | exception_handling | [`ExceptionHandling`](#exceptionhandling) | Exception handling configuration. |
      
      #### ExceptionHandling
      
      | Sub-Key | Type | Description |
      | ------- | ---- | ----------- |
      | error_code | String | The error code. |
      | target_table_name | String | The name of the target table. |
      | exception_handling_sql_path | String | The file path for exception handling SQL query. |
      | update_metadata_query_path | String | The file path for metadata update query if any. |
      | source_view | String | The source view which should be used by exception_handling_sql_path. |
      | data_source_name | String | The name of the data source  specified in `name` of [`DataSourceConfig`](#datasourceconfig)  |
      
