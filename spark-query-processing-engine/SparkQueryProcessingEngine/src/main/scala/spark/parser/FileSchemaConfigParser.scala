package spark.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import spark.Util
import spark.metadata.FileSchemaConfig

/** Object responsible for parsing a JSON file containing an array of `FileSchemaConfig` objects.
  */
object FileSchemaConfigParser {

  /** Parses the JSON file at the specified `path` and returns an array of `FileSchemaConfig` objects.
    *
    * @param path The path to the JSON file containing the configuration for file schemas.
    * @return An array of `FileSchemaConfig` objects parsed from the JSON file.
    */
  def apply(path: String): Array[FileSchemaConfig] = {
    // Creating an ObjectMapper with Scala module
    val mapper: ObjectMapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Reading the JSON content from the specified path
    val tableMappingJson: String = Util.readFileFromResourcePath(path)

    // Mapping the JSON content to an array of FileSchemaConfig objects
    mapper.readValue(tableMappingJson, classOf[Array[FileSchemaConfig]])
  }

}
