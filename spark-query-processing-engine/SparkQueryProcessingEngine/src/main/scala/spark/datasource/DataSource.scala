package spark.datasource

import org.apache.spark.sql.DataFrame
import spark.metadata.FileSchemaConfig

import scala.collection.mutable

/** A trait representing a generic data source abstraction.
  * Classes extending this trait must provide specific implementations for data reading, writing, and configuration.
  */
trait DataSource {

  def loadQuery(source: String): DataFrame

  def write(target: String, df: DataFrame): Unit

  def loadDataFromSource(source: Any, fileSchema: FileSchemaConfig = null): DataFrame

  def getConfig: Map[String, String] = {
    makeOptions().toMap[String, String]
  }
  def makeOptions(): mutable.Map[String, String]

}
