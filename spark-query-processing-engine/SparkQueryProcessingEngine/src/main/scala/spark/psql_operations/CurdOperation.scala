package spark.psql_operations

import org.slf4j.Logger
import spark.datasource.DataSource

import java.sql.{Connection, DriverManager, Statement}
import scala.collection.mutable.ListBuffer

/** Object containing CRUD operations for PostgreSQL.
  */
object CurdOperation {

  val driver = "org.postgresql.Driver"

  /** Performs Data Manipulation Language (DML) operation (CREATE, UPDATE, DELETE, INSERT) on the database.
    *
    * @param dataSource The DataSource containing connection configuration.
    * @param action     The type of DML operation (CREATE, UPDATE, DELETE, INSERT).
    * @param statement  The SQL statement for the DML operation.
    * @param logger     The logger for logging messages.
    */
  def performDmlOperation(dataSource: DataSource, action: String, statement: String)(implicit logger: Logger): Unit = {
    val pgConn = connection(dataSource.getConfig)

    action.toUpperCase match {
      case "CREATE" => actionCreate(pgConn, statement)(logger)
      case "UPDATE" => actionUPDATE(pgConn, statement)(logger)
      case "DELETE" => actionDelete(pgConn, statement)(logger)
      case "INSERT" => actionInsert(pgConn, statement)(logger)
    }
    pgConn.close()
  }

  /** Performs the CREATE operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param statement      The SQL statement for CREATE operation.
    * @param logger         The logger for logging messages.
    * @return True if the operation is successful, false otherwise.
    */
  def actionCreate(psqlConnection: Connection, statement: String)(implicit logger: Logger): Boolean = {
    val prepareStatement = psqlConnection.prepareStatement(statement)
    prepareStatement.executeUpdate()
    prepareStatement.close()
    true
  }

  /** Performs the UPDATE operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param statement      The SQL statement for UPDATE operation.
    * @param logger         The logger for logging messages.
    * @return True if the operation is successful, false otherwise.
    */
  def actionUPDATE(psqlConnection: Connection, statement: String)(implicit logger: Logger): Boolean = {
    val prepareStatementAddColumn = psqlConnection.prepareStatement(statement)
    prepareStatementAddColumn.executeUpdate()
    prepareStatementAddColumn.close()
    true
  }

  /** Performs the DELETE operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param statement      The SQL statement for DELETE operation.
    * @param logger         The logger for logging messages.
    * @return True if the operation is successful, false otherwise.
    */
  def actionDelete(psqlConnection: Connection, statement: String)(implicit logger: Logger): Boolean = {
    val prepareStatementAddColumn = psqlConnection.prepareStatement(statement)
    prepareStatementAddColumn.executeUpdate()
    prepareStatementAddColumn.close()
    true
  }

  /** Performs the INSERT operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param statement      The SQL statement for INSERT operation.
    * @param logger         The logger for logging messages.
    * @return True if the operation is successful, false otherwise.
    */
  def actionInsert(psqlConnection: Connection, statement: String)(implicit logger: Logger): Boolean = {
    val prepareStatementAddColumn = psqlConnection.prepareStatement(statement)
    prepareStatementAddColumn.executeUpdate()
    prepareStatementAddColumn.close()
    true
  }

  /** Performs batch UPDATE operation in the database.
    *
    * @param dataSource    The DataSource containing connection configuration.
    * @param action        The type of DML operation (UPDATE).
    * @param updateQueries An array of SQL statements for batch UPDATE operation.
    * @param logger        The logger for logging messages.
    */
  def performUpdateBatchOperation(dataSource: DataSource, action: String, updateQueries: Array[String])(implicit
      logger: Logger
  ): Unit = {
    val pgConn = connection(dataSource.getConfig)

    action match {
      case "UPDATE" => actionUpdateBatch(pgConn, updateQueries)
    }
    pgConn.close()
  }

  /** Performs batch UPDATE operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param updateQueries  An array of SQL statements for batch UPDATE operation.
    * @param logger         The logger for logging messages.
    */
  def actionUpdateBatch(psqlConnection: Connection, updateQueries: Array[String])(implicit logger: Logger): Unit = {
    val statement: Statement = psqlConnection.createStatement()
    updateQueries.foreach { query =>
      statement.addBatch(query)
    }
    statement.executeBatch()
  }

  /** Performs Data Query Language (DQL) operation (READ) on the database.
    *
    * @param dataSource The DataSource containing connection configuration.
    * @param statement  The SQL statement for the DQL operation.
    * @param logger     The logger for logging messages.
    * @return A list of lists representing the result of the READ operation.
    */
  def performDqlOperation(dataSource: DataSource, statement: String)(implicit logger: Logger): List[List[String]] = {
    val pgConn = connection(dataSource.getConfig)
    val data = actionRead(pgConn, statement)(logger)
    pgConn.close()
    data
  }

  /** Establishes a connection to the PostgreSQL database.
    *
    * @param connectionConfig Configuration parameters for the database connection.
    * @return A connection to the PostgreSQL database.
    */
  def connection(connectionConfig: Map[String, String]): Connection = {
    Class.forName(driver)
    val url = connectionConfig.getOrElse("url", "")
    val dbUser = connectionConfig.getOrElse("user", "")
    val dbPassword = connectionConfig.getOrElse("password", "")
    DriverManager.getConnection(url, dbUser, dbPassword)
  }

  /** Performs the READ operation in the database.
    *
    * @param psqlConnection The PostgreSQL database connection.
    * @param statement      The SQL statement for READ operation.
    * @param logger         The logger for logging messages.
    * @return A list of lists representing the result of the READ operation.
    */
  def actionRead(psqlConnection: Connection, statement: String)(implicit logger: Logger): List[List[String]] = {
    val psqlStatement = psqlConnection.createStatement()
    val resultSet = psqlStatement.executeQuery(statement)
    val resultSetMetadata = resultSet.getMetaData
    val columnCount = resultSetMetadata.getColumnCount
    val allColumns = new ListBuffer[String]()

    // Get column names
    for (i <- 1 to columnCount) {
      val column_name = resultSetMetadata.getColumnName(i)
      allColumns += column_name
    }

    val allColumnsList = allColumns.toList
    val allColumnValues = new ListBuffer[List[String]]()

    // Get column values
    while (resultSet.next()) {
      val rec = new ListBuffer[String]()
      for (c <- allColumnsList) {
        val value = resultSet.getString(c)
        rec += value
      }
      allColumnValues += rec.toList
    }

    resultSet.close()
    psqlStatement.close()
    allColumnValues.toList
  }
}
