name := "deltalite"
version := "0.1.0"
scalaVersion := "2.12.10"

// Define Spark dependencies with 'provided' scope
// These won't be included in the JAR as they'll be provided by the Spark environment
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided",
  
  // Testing dependencies
  "org.scalatest" %% "scalatest" % "3.2.2" % "test"
)

// Explicitly specify JDK 11 compatibility
javacOptions ++= Seq("-source", "11", "-target", "11")

// Assembly settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
