#!/bin/bash

# Define project parameters
PROJECT_NAME="deltalite"
SCALA_VERSION="2.12.10"
SBT_VERSION="1.3.10"  # Same as what Delta uses
SPARK_VERSION="3.0.0" # Delta 1.0.1 uses Spark 3.0.0

# Create project directory
mkdir -p $PROJECT_NAME
cd $PROJECT_NAME

# Create directory structure
mkdir -p src/main/scala/com/example/deltalite
mkdir -p src/test/scala/com/example/deltalite
mkdir -p project

# Create build.sbt
cat > build.sbt << EOL
name := "$PROJECT_NAME"
version := "0.1.0"
scalaVersion := "$SCALA_VERSION"

// Define Spark dependencies with 'provided' scope
// These won't be included in the JAR as they'll be provided by the Spark environment
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "$SPARK_VERSION" % "provided",
  "org.apache.spark" %% "spark-sql" % "$SPARK_VERSION" % "provided",
  
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
EOL

# Create project/build.properties
cat > project/build.properties << EOL
sbt.version=$SBT_VERSION
EOL

# Create project/plugins.sbt
cat > project/plugins.sbt << EOL
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")
EOL

# Create SBT wrapper script (sbtw)
cat > sbtw << EOL
#!/bin/bash

# Check for Java version (must be JDK 11)
JAVA_VERSION=\$(java -version 2>&1 | grep -i version | sed 's/.*version "\\([0-9]*\\)\\.[0-9]*\\.[0-9]*\\(_.*\\)\\{0,1\\}".*/\\1/g')

if [ "\$JAVA_VERSION" != "11" ]; then
  echo "Error: JDK 11 is required to build this project. Current version: \$JAVA_VERSION"
  echo "Please install JDK 11 and ensure it's in your PATH."
  exit 1
else
  echo "Using JDK 11 as required."
fi

# SBT version to download if not present
SBT_VERSION="$SBT_VERSION"

# Download location for SBT
SBT_LAUNCHER_DIR="\$HOME/.sbt/launchers/\$SBT_VERSION"
SBT_LAUNCHER_JAR="\$SBT_LAUNCHER_DIR/sbt-launch.jar"

# Download SBT if not already present
if [ ! -f "\$SBT_LAUNCHER_JAR" ]; then
  echo "Downloading SBT \$SBT_VERSION..."
  mkdir -p "\$SBT_LAUNCHER_DIR"
  curl -L "https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/\$SBT_VERSION/sbt-launch-\$SBT_VERSION.jar" -o "\$SBT_LAUNCHER_JAR"
  if [ \$? -ne 0 ]; then
    echo "Error downloading SBT. Please check your internet connection and try again."
    exit 1
  fi
fi

# Run SBT with the specified arguments
java -Xms512M -Xmx1536M -Xss2M -XX:+UseG1GC -XX:ReservedCodeCacheSize=256M -Dsbt.log.noformat=true -jar "\$SBT_LAUNCHER_JAR" "\$@"
EOL

# Make the wrapper executable
chmod +x sbtw

# Create sample application
cat > src/main/scala/com/example/deltalite/DeltaLite.scala << EOL
package com.example.deltalite

import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

object DeltaLite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaLite")
      .master("local[*]")
      .getOrCreate()
    
    println("Initialized DeltaLite with Spark " + spark.version)
    
    spark.stop()
  }
}
EOL

# Create a basic test
cat > src/test/scala/com/example/deltalite/DeltaLiteTest.scala << EOL
package com.example.deltalite

import org.scalatest.funsuite.AnyFunSuite

class DeltaLiteTest extends AnyFunSuite {
  test("DeltaLite should initialize") {
    assert(true)  // Placeholder for real tests
  }
}
EOL

# Create README.md with project instructions
cat > README.md << EOL
# DeltaLite

A lightweight implementation of Delta Lake concepts for educational purposes.

## Requirements

- JDK 11
- Internet connection (for first build to download SBT and dependencies)

## Getting Started

This project includes a custom SBT wrapper script (\`sbtw\`) that checks for JDK 11 
and downloads the correct SBT version if needed.

### Basic Commands

Compile the project:
\`\`\`
./sbtw compile
\`\`\`

Run tests:
\`\`\`
./sbtw test
\`\`\`

Create a fat JAR:
\`\`\`
./sbtw assembly
\`\`\`

Run the example with spark-submit:
\`\`\`
spark-submit target/scala-$SCALA_VERSION/$PROJECT_NAME-assembly-0.1.0.jar
\`\`\`

## Project Structure

- \`src/main/scala/\`: Source code
- \`src/test/scala/\`: Test code
- \`build.sbt\`: Project configuration
- \`project/\`: SBT configuration files
EOL

# Create .gitignore
cat > .gitignore << EOL
target/
project/target/
project/project/
.idea/
.bsp/
*.iml
*.class
*.log
.DS_Store
EOL

echo "Project $PROJECT_NAME has been created with Scala $SCALA_VERSION and SBT $SBT_VERSION"
echo "The project requires JDK 11 to build."
echo ""
echo "To build the project:"
echo "  cd $PROJECT_NAME"
echo "  ./sbtw compile"
echo ""
echo "To run tests:"
echo "  ./sbtw test"
echo ""
echo "To create a fat JAR:"
echo "  ./sbtw assembly"
echo ""
echo "To run the example with spark-submit:"
echo "  spark-submit target/scala-$SCALA_VERSION/$PROJECT_NAME-assembly-0.1.0.jar"
