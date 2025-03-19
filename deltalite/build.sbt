name := "deltalite"
version := "0.1.0"
scalaVersion := "2.12.10"

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.2" % "test"
)

// Explicitly specify JDK 11 compatibility
javacOptions ++= Seq("-source", "11", "-target", "11")

// Assembly settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
