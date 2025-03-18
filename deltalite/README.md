# DeltaLite

A lightweight implementation of Delta Lake concepts for educational purposes.

## Requirements

- JDK 11
- Internet connection (for first build to download SBT and dependencies)

## Getting Started

This project includes a custom SBT wrapper script (`sbtw`) that checks for JDK 11 
and downloads the correct SBT version if needed.

### Basic Commands

Compile the project:
```
./sbtw compile
```

Run tests:
```
./sbtw test
```

Create a fat JAR:
```
./sbtw assembly
```

Run the example with spark-submit:
```
spark-submit target/scala-2.12.10/deltalite-assembly-0.1.0.jar
```

## Project Structure

- `src/main/scala/`: Source code
- `src/test/scala/`: Test code
- `build.sbt`: Project configuration
- `project/`: SBT configuration files
