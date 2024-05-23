package spark.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import spark.Util
import spark.metadata.JobConfig

/**
 * Object responsible for parsing a JSON file containing a `JobConfig` object.
 */
object JobConfigParser {

  /**
   * Parses the JSON file at the specified `path` and returns a `JobConfig` object.
   *
   * @param path The path to the JSON file containing the configuration for a job.
   * @return A `JobConfig` object parsed from the JSON file.
   */
  def apply(path: String): JobConfig = {
    // Creating an ObjectMapper with Scala module
    val mapper: ObjectMapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Reading the JSON content from the specified path
    val tableMappingJson: String = Util.readFileFromResourcePath(path)

    // Mapping the JSON content to a JobConfig object
    mapper.readValue(tableMappingJson, classOf[JobConfig])
  }

}
