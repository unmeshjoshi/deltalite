package com.example.deltalite

import com.example.deltalite.actions._
import com.example.deltalite.io.SimpleFileFormat
import java.io.File
import java.util.UUID

/**
 * High-level API for working with a DeltaLite table.
 * Provides methods for CRUD operations on the table.
 */
class DeltaLiteTable(val path: String) {
  
  private val _deltaLog = DeltaLiteLog.forTable(path)
  
  /**
   * Get the underlying DeltaLiteLog for this table.
   * Useful for low-level operations and direct access to checkpointing functionality.
   */
  def deltaLog: DeltaLiteLog = _deltaLog
  
  /**
   * Get the current version of the table.
   */
  def version: Long = _deltaLog.snapshot.version
  
  /**
   * Get the current table metadata.
   */
  def metadata: Option[Metadata] = {
    DeltaLiteTable.readTableMetadata(this)
  }
  
  /**
   * Create a new table with the given schema and configuration.
   * 
   * @param name Table name
   * @param schemaJson JSON schema string
   * @param partitionColumns List of partition column names
   * @param configuration Table configuration
   * @return Version of the new table
   */
  def createTable(
      name: String, 
      schemaJson: String, 
      partitionColumns: Seq[String] = Seq.empty,
      configuration: Map[String, String] = Map.empty
  ): Long = {
    val txn = _deltaLog.startTransaction()
    
    val metadata = Metadata(
      name = name,
      description = s"Table $name",
      schemaString = schemaJson,
      partitionColumns = partitionColumns,
      configuration = configuration,
      createdTime = System.currentTimeMillis()
    )
    
    txn.commit(Seq(metadata), "CREATE TABLE")
  }
  
  /**
   * Insert records into the table.
   * 
   * @param records Records to insert as maps
   * @param partitionValues Partition values for these records
   * @return Version after the insert
   */
  def insert(
      records: Seq[Map[String, String]],
      partitionValues: Map[String, String] = Map.empty
  ): Long = {
    val txn = _deltaLog.startTransaction()
    
    // Write records to a file
    val addFile = txn.writeFile(records, partitionValues)
    
    txn.commit(Seq(addFile), "INSERT")
  }
  
  /**
   * Insert records into the table partitioned by the given partition columns.
   * 
   * @param records Records to insert
   * @param partitionBy Function to extract partition values from a record
   * @return Version after the insert
   */
  def insertPartitioned(
      records: Seq[Map[String, String]],
      partitionBy: Map[String, String] => Map[String, String]
  ): Long = {
    val txn = _deltaLog.startTransaction()
    
    // Group records by partition values
    val recordsByPartition = records.groupBy(partitionBy)
    
    // Write files for each partition
    val addFiles = txn.writeFiles(recordsByPartition)
    
    txn.commit(addFiles, "INSERT")
  }
  
  /**
   * Read all records from the table.
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @return All records in the table
   */
  def readAll[T](converter: Map[String, String] => T): Seq[T] = {
    val snapshot = _deltaLog.snapshot
    
    snapshot.state.readRecords(converter)
  }
  
  /**
   * Find records matching a predicate.
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @param predicate Function to filter records
   * @return Matching records
   */
  def find[T](
      converter: Map[String, String] => T, 
      predicate: T => Boolean
  ): Seq[T] = {
    val snapshot = _deltaLog.snapshot
    
    snapshot.state.findRecords(converter, predicate)
  }
  
  /**
   * Update records meeting a condition.
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @param recordToMap Function to convert T to Map[String, String]
   * @param condition Function to identify records to update
   * @param update Function to update a record
   * @return Version after the update
   */
  def update[T](
      converter: Map[String, String] => T,
      recordToMap: T => Map[String, String],
      condition: T => Boolean,
      update: T => T
  ): Long = {
    val txn = _deltaLog.startTransaction()
    
    // Get current snapshot and files
    val snapshot = txn.snapshot
    val files = snapshot.allFiles
    
    // Mark files as read for conflict detection
    txn.readFiles(files.map(_.path))
    
    // Read all records
    val allRecords = files.flatMap { file =>
      val records = SimpleFileFormat.readRecords(file.path)
      records.map(converter)
    }
    
    // Split records into those to update and those to keep unchanged
    val (recordsToUpdate, unchangedRecords) = allRecords.partition(condition)
    
    if (recordsToUpdate.isEmpty) {
      // No records to update, just return current version
      return snapshot.version
    }
    
    // Update the records
    val updatedRecords = recordsToUpdate.map(update)
    
    // Convert all records back to maps
    val allUpdatedRecords = (unchangedRecords ++ updatedRecords).map(recordToMap)
    
    // Create actions: remove old files and add new file with updated data
    val removeActions = files.map(f => 
      RemoveFile(f.path, Some(System.currentTimeMillis()))
    )
    
    // Write new file with all updated records
    val addFile = txn.writeFile(allUpdatedRecords)
    
    // Commit the transaction
    txn.commit(removeActions :+ addFile, "UPDATE")
  }
  
  /**
   * Delete records meeting a condition.
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @param recordToMap Function to convert T to Map[String, String]
   * @param condition Function to identify records to delete
   * @return Version after the delete
   */
  def delete[T](
      converter: Map[String, String] => T,
      recordToMap: T => Map[String, String],
      condition: T => Boolean
  ): Long = {
    val txn = _deltaLog.startTransaction()
    
    // Get current snapshot and files
    val snapshot = txn.snapshot
    val files = snapshot.allFiles
    
    // Mark files as read for conflict detection
    txn.readFiles(files.map(_.path))
    
    // Read all records
    val allRecords = files.flatMap { file =>
      val records = SimpleFileFormat.readRecords(file.path)
      records.map(converter)
    }
    
    // Keep only records that don't match the condition
    val remainingRecords = allRecords.filterNot(condition)
    
    if (remainingRecords.size == allRecords.size) {
      // No records to delete, just return current version
      return snapshot.version
    }
    
    // Convert remaining records back to maps
    val remainingMaps = remainingRecords.map(recordToMap)
    
    // Create actions: remove old files
    val removeActions = files.map(f => 
      RemoveFile(f.path, Some(System.currentTimeMillis()))
    )
    
    // Add action to write remaining records (if any)
    val addActions = if (remainingRecords.isEmpty) {
      Seq.empty
    } else {
      Seq(txn.writeFile(remainingMaps))
    }
    
    // Commit the transaction
    txn.commit(removeActions ++ addActions, "DELETE")
  }
  
  /**
   * Execute a custom transaction on the table.
   * 
   * @param isolation Isolation level for the transaction
   * @param operation Operation name for logging
   * @param transactionBody Function that takes a transaction and performs operations
   * @return Version after the transaction
   */
  def executeTransaction(
      isolation: IsolationLevel = Serializable,
      operation: String = "CUSTOM",
      transactionBody: OptimisticTransaction => Seq[Action]
  ): Long = {
    val txn = new OptimisticTransaction(_deltaLog, isolation)
    
    // Execute the transaction body to get actions
    val actions = transactionBody(txn)
    
    // Commit the transaction
    txn.commit(actions, operation)
  }
  
  /**
   * Get the history of operations on this table.
   * 
   * @param limit Maximum number of versions to return (0 for all)
   * @return Sequence of (version, operation, timestamp) tuples
   */
  def history(limit: Int = 0): Seq[(Long, String, Long)] = {
    val versions = _deltaLog.listVersions()
    
    val limitedVersions = if (limit > 0) versions.takeRight(limit) else versions
    
    limitedVersions.flatMap { version =>
      val commitInfoOpt = _deltaLog.readVersion(version)
        .find(_.contains("\"commitInfo\""))
        .flatMap { line =>
          try {
            val timestamp = DeltaLiteTable.extractJsonValue(line, "timestamp").toLong
            val operation = DeltaLiteTable.extractJsonValue(line, "operation")
            Some((version, operation, timestamp))
          } catch {
            case _: Exception => None
          }
        }
      
      commitInfoOpt.orElse(Some((version, "UNKNOWN", 0L)))
    }
  }
  
  /**
   * Read records from the table at a specific version (time travel).
   * 
   * @param version The version to read
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @return Records in the table at the specified version
   */
  def timeTravel[T](
      version: Long, 
      converter: Map[String, String] => T
  ): Seq[T] = {
    // Validate version exists
    val availableVersions = _deltaLog.listVersions()
    if (version < 0 || !availableVersions.contains(version)) {
      throw new IllegalArgumentException(
        s"Version $version does not exist. Available versions: ${availableVersions.sorted.mkString(", ")}")
    }
    
    // Get a snapshot at the requested version
    val snapshot = new Snapshot(version, _deltaLog)
    
    // Read records from this snapshot
    snapshot.state.readRecords(converter)
  }
  
  /**
   * Find records matching a predicate at a specific version (time travel).
   * 
   * @param version The version to read
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @param predicate Function to filter records
   * @return Matching records at the specified version
   */
  def timeTravelAndFind[T](
      version: Long,
      converter: Map[String, String] => T, 
      predicate: T => Boolean
  ): Seq[T] = {
    // Validate version exists
    val availableVersions = _deltaLog.listVersions()
    if (version < 0 || !availableVersions.contains(version)) {
      throw new IllegalArgumentException(
        s"Version $version does not exist. Available versions: ${availableVersions.sorted.mkString(", ")}")
    }
    
    // Get a snapshot at the requested version
    val snapshot = new Snapshot(version, _deltaLog)
    
    // Find records from this snapshot
    snapshot.state.findRecords(converter, predicate)
  }
}

object DeltaLiteTable {
  /**
   * Create a DeltaLiteTable for the given path.
   */
  def forPath(path: String): DeltaLiteTable = {
    new DeltaLiteTable(path)
  }

  /**
   * Helper to extract values from JSON strings.
   */
  def extractJsonValue(json: String, key: String): String = {
    val keyPattern = "\"" + key + "\":"
    if (json.contains(keyPattern)) {
      if (json.contains("\"" + key + "\":\"")) {
        // String value
        val parts = json.split("\"" + key + "\":\"")
        val valuePart = parts.last
        val endQuotePos = valuePart.indexOf("\"")
        if (endQuotePos >= 0) {
          valuePart.substring(0, endQuotePos)
        } else {
          ""
        }
      } else {
        // Numeric or boolean value
        val parts = json.split("\"" + key + "\":")
        val valuePart = parts.last.trim
        val endPos = Math.min(
          if (valuePart.indexOf(",") >= 0) valuePart.indexOf(",") else Int.MaxValue,
          if (valuePart.indexOf("}") >= 0) valuePart.indexOf("}") else Int.MaxValue
        )
        if (endPos < Int.MaxValue) {
          valuePart.substring(0, endPos)
        } else {
          valuePart
        }
      }
    } else {
      ""
    }
  }

  /**
   * Read table metadata
   */
  def readTableMetadata(table: DeltaLiteTable): Option[Metadata] = {
    // Read the version file directly for the metadata
    val logPath = s"${table.path}/_delta_log"
    val versionsFiles = new File(logPath).listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".json"))
      .sortBy(_.getName)
    
    if (versionsFiles.isEmpty) {
      return None
    }
    
    // Check only the first version file which should contain the metadata
    val latestVersionFile = versionsFiles.head
    val lines = scala.io.Source.fromFile(latestVersionFile).getLines().mkString("\n")
    
    // Look for metadata action in the file
    if (lines.contains("\"metadata\"")) {
      try {
        // Parse the full metadata JSON
        val metadataStart = lines.indexOf("{\"metadata\":")
        val metadataEnd = lines.indexOf("}", metadataStart + 50) + 1
        val metadataJson = lines.substring(metadataStart, metadataEnd)
        
        // Parse the ID, name, description directly
        val id = extractJsonValue(metadataJson, "id")
        val name = extractJsonValue(metadataJson, "name")
        val description = extractJsonValue(metadataJson, "description")
        
        // Extract the schema string - this is more complex since it's a nested JSON object
        val schemaStart = lines.indexOf("\"schemaString\":")
        if (schemaStart > 0) {
          val contentStart = schemaStart + "\"schemaString\":".length
          
          // Count braces to find the matching end of the schema JSON object
          var openBraces = 0
          var startPos = contentStart
          var schemaContent = ""
          
          // Skip whitespace to find the opening brace
          while (startPos < lines.length && lines.charAt(startPos) != '{') {
            startPos += 1
          }
          
          if (startPos < lines.length) {
            var i = startPos
            var inQuotes = false
            var escaped = false
            
            while (i < lines.length && (openBraces > 0 || i == startPos)) {
              val c = lines.charAt(i)
              
              if (c == '"' && !escaped) {
                inQuotes = !inQuotes
              } else if (!inQuotes) {
                if (c == '{') {
                  openBraces += 1
                } else if (c == '}') {
                  openBraces -= 1
                }
              }
              
              escaped = c == '\\' && !escaped
              
              if (openBraces >= 0) {
                i += 1
              }
            }
            
            // Extract the schema content
            schemaContent = lines.substring(startPos, i)
          }
          
          // Extract partition columns and configuration
          val partitionColumnsStr = extractJsonValue(metadataJson, "partitionColumns")
          val partitionColumns = if (partitionColumnsStr.nonEmpty) {
            partitionColumnsStr.split(",").toSeq
          } else {
            Seq.empty[String]
          }
          
          // Get the configuration map directly from the metadata JSON
          val configurationStart = lines.indexOf("\"configuration\":")
          val config = if (configurationStart > 0) {
            // Extract the configuration value section
            val configContentStart = configurationStart + "\"configuration\":".length
            val configContent = lines.substring(configContentStart).trim
            
            if (configContent.startsWith("{")) {
              // It's a JSON object, find the matching closing brace
              var openBraces = 1
              var pos = 1
              
              while (pos < configContent.length && openBraces > 0) {
                val c = configContent.charAt(pos)
                if (c == '{') openBraces += 1
                else if (c == '}') openBraces -= 1
                pos += 1
              }
              
              if (openBraces == 0) {
                val configJson = configContent.substring(0, pos)
                // Parse the configuration map
                val versionPattern = "\"deltalite.version\"\\s*:\\s*\"([^\"]*)\"".r
                val versionValue = versionPattern.findFirstMatchIn(configJson).map(_.group(1))
                
                versionValue.map(v => Map("deltalite.version" -> v)).getOrElse(Map.empty[String, String])
              } else {
                Map.empty[String, String]
              }
            } else {
              Map.empty[String, String]
            }
          } else {
            Map.empty[String, String]
          }
          
          println(s"Extracted configuration: $config")
          
          val createdTimeStr = extractJsonValue(metadataJson, "createdTime")
          val createdTime = if (createdTimeStr.nonEmpty) createdTimeStr.toLong else System.currentTimeMillis()
          
          println(s"Extracted schema content: $schemaContent")
          
          Some(Metadata(
            id = id,
            name = name,
            description = description,
            schemaString = schemaContent,
            partitionColumns = partitionColumns,
            configuration = config,
            createdTime = createdTime
          ))
        } else {
          println("Could not find schemaString in metadata")
          None
        }
      } catch {
        case e: Exception =>
          println(s"Error parsing metadata: ${e.getMessage}")
          e.printStackTrace()
          None
      }
    } else {
      println("No metadata action found in log file")
      None
    }
  }
} 