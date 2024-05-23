ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"
val postgresqlVersion = "42.5.4"

// Note the dependencies are provided
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.postgresql" % "postgresql" % postgresqlVersion % Provided

)

lazy val root = (project in file("."))
  .settings(
    name := "SparkQueryProcessingEngine"
  )
