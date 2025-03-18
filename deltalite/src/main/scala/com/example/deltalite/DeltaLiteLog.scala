package com.example.deltalite

import com.example.deltalite.actions._
import java.io.{File, FileWriter, BufferedWriter, FileReader, BufferedReader, FileOutputStream, ObjectOutputStream, FileInputStream, ObjectInputStream}
import java.util.concurrent.locks.ReentrantLock
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex
import java.util.UUID

/**
 * Main entry point for using DeltaLite tables. Manages the transaction log
 * and provides access to the current state of the table.
 */
object DeltaLiteLog {
  /**
   * Creates a DeltaLiteLog for the given table path.
   */
  def forTable(tablePath: String): DeltaLiteLog = {
    new DeltaLiteLog(tablePath)
  }
  
  // Default checkpoint interval (create a checkpoint every N versions)
  val DEFAULT_CHECKPOINT_INTERVAL = 10
}

class DeltaLiteLog(val tablePath: String) {
  import DeltaLiteLog._
  
  // Path to the transaction log directory
  val logPath: String = s"$tablePath/_delta_log"
  
  // Path to the checkpoints directory
  val checkpointPath: String = s"$logPath/_checkpoints"
  
  // Lock for synchronizing access to the log
  private val logLock = new ReentrantLock()
  
  // Current snapshot of the table
  @volatile private var _snapshot: Snapshot = _
  
  // Interval at which to create checkpoints (every N versions)
  private var _checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL
  
  /**
   * Get the checkpoint interval (the number of versions between checkpoints)
   */
  def checkpointInterval: Int = _checkpointInterval
  
  /**
   * Set the checkpoint interval
   */
  def checkpointInterval_=(interval: Int): Unit = {
    require(interval > 0, "Checkpoint interval must be positive")
    _checkpointInterval = interval
  }
  
  /**
   * Get the current snapshot of the table.
   */
  def snapshot: Snapshot = {
    if (_snapshot == null) {
      _snapshot = update()
    }
    _snapshot
  }
  
  /**
   * Updates the state of the log by reading new version files.
   * @return Updated snapshot
   */
  def update(): Snapshot = {
    logLock.lock()
    try {
      val versions = listVersions()
      val latestVersion = if (versions.isEmpty) 0L else versions.max
      
      if (_snapshot == null || _snapshot.version < latestVersion) {
        // Try to load from checkpoint first if available
        val snapshotFromCheckpoint = if (_snapshot == null) {
          loadLatestCheckpoint(latestVersion)
        } else {
          loadLatestCheckpoint(latestVersion, _snapshot.version + 1)
        }
        
        _snapshot = snapshotFromCheckpoint.getOrElse(new Snapshot(latestVersion, this))
        
        // Check if we need to create a new checkpoint
        if (shouldCheckpoint(latestVersion)) {
          checkpoint(latestVersion)
        }
      }
      
      _snapshot
    } finally {
      logLock.unlock()
    }
  }
  
  /**
   * Determine if we should create a checkpoint for the given version
   */
  private def shouldCheckpoint(version: Long): Boolean = {
    if (version < 0) return false
    
    // Check if version is divisible by the checkpoint interval
    version % _checkpointInterval == 0 && !checkpointExists(version)
  }
  
  /**
   * Check if a checkpoint exists for the given version
   */
  private def checkpointExists(version: Long): Boolean = {
    val checkpointFile = new File(s"$checkpointPath/$version.checkpoint")
    checkpointFile.exists()
  }
  
  /**
   * Create a checkpoint for the given version
   */
  def checkpoint(version: Long): Boolean = {
    logLock.lock()
    try {
      val checkpointDir = new File(checkpointPath)
      if (!checkpointDir.exists()) {
        checkpointDir.mkdirs()
      }
      
      // Create the checkpoint file
      val checkpointFile = new File(s"$checkpointPath/$version.checkpoint")
      
      // Serialize the TableState
      val snapshot = new Snapshot(version, this)
      val state = snapshot.state
      
      // Use Java serialization for simplicity
      // In a real implementation, you'd use a more efficient format
      val oos = new ObjectOutputStream(new FileOutputStream(checkpointFile))
      try {
        // Write version
        oos.writeLong(version)
        
        // Write metadata if present
        oos.writeBoolean(state.metadata.isDefined)
        state.metadata.foreach { metadata =>
          oos.writeUTF(metadata.json)
        }
        
        // Write commit info if present
        oos.writeBoolean(state.commitInfo.isDefined)
        state.commitInfo.foreach { commitInfo =>
          oos.writeUTF(commitInfo.json)
        }
        
        // Write files
        val files = state.files
        oos.writeInt(files.size)
        files.foreach { file =>
          oos.writeUTF(file.json)
        }
        
        true
      } finally {
        oos.close()
      }
    } catch {
      case e: Exception =>
        println(s"Error creating checkpoint: ${e.getMessage}")
        false
    } finally {
      logLock.unlock()
    }
  }
  
  /**
   * Load the latest checkpoint less than or equal to the target version
   * 
   * @param targetVersion The target version to load
   * @param startVersion Optional starting version to consider (for incremental updates)
   * @return Option containing a snapshot from the checkpoint
   */
  private def loadLatestCheckpoint(targetVersion: Long, startVersion: Long = 0): Option[Snapshot] = {
    val checkpointDir = new File(checkpointPath)
    if (!checkpointDir.exists()) {
      return None
    }
    
    // Find all checkpoint files
    val checkpointFiles = checkpointDir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".checkpoint"))
      .map(f => f.getName.stripSuffix(".checkpoint").toLong)
      .filter(v => v <= targetVersion && v >= startVersion)
      .sorted
    
    if (checkpointFiles.isEmpty) {
      return None
    }
    
    // Get the latest checkpoint
    val latestCheckpoint = checkpointFiles.max
    val checkpointFile = new File(s"$checkpointPath/$latestCheckpoint.checkpoint")
    
    Try {
      val ois = new ObjectInputStream(new FileInputStream(checkpointFile))
      try {
        // Read version
        val version = ois.readLong()
        
        // Create a new state
        val state = new TableState(version)
        
        // Read metadata
        val hasMetadata = ois.readBoolean()
        if (hasMetadata) {
          val metadataJson = ois.readUTF()
          val metadata = parseMetadata(metadataJson)
          metadata.foreach(state.applyAction)
        }
        
        // Read commit info
        val hasCommitInfo = ois.readBoolean()
        if (hasCommitInfo) {
          val commitInfoJson = ois.readUTF()
          val commitInfo = parseCommitInfo(commitInfoJson)
          commitInfo.foreach(state.applyAction)
        }
        
        // Read files
        val fileCount = ois.readInt()
        for (_ <- 0 until fileCount) {
          val fileJson = ois.readUTF()
          val file = parseAddFile(fileJson)
          file.foreach(state.applyAction)
        }
        
        // Create a snapshot with the loaded state
        // If the checkpoint is not for the target version, we need to apply the remaining versions
        val snapshot = new Snapshot(version, this, Some(state))
        
        // If the checkpoint is older than the target version, we need to apply the newer versions
        if (version < targetVersion) {
          val remainingVersions = listVersions().filter(v => v > version && v <= targetVersion).sorted
          
          // Apply actions from remaining versions
          for {
            ver <- remainingVersions
            actionStr <- readVersion(ver)
            action <- parseAction(actionStr)
          } {
            state.applyAction(action)
          }
          
          // Update the version
          new Snapshot(targetVersion, this, Some(state))
        } else {
          snapshot
        }
      } finally {
        ois.close()
      }
    }.toOption
  }
  
  /**
   * Parse an action from a JSON string.
   */
  private def parseAction(actionStr: String): Option[Action] = {
    if (actionStr.contains("\"add\"")) {
      parseAddFile(actionStr)
    } else if (actionStr.contains("\"remove\"")) {
      parseRemoveFile(actionStr)
    } else if (actionStr.contains("\"metadata\"")) {
      parseMetadata(actionStr)
    } else if (actionStr.contains("\"commitInfo\"")) {
      parseCommitInfo(actionStr)
    } else {
      None
    }
  }
  
  /**
   * Parse AddFile action from JSON string.
   */
  private def parseAddFile(actionStr: String): Option[AddFile] = {
    try {
      // Parse AddFile
      val path = extractJsonValue(actionStr, "path")
      val sizeStr = extractJsonValue(actionStr, "size")
      val modTimeStr = extractJsonValue(actionStr, "modificationTime")
      val dataChangeStr = extractJsonValue(actionStr, "dataChange")
      
      // Only add if we can extract the required fields
      if (path != null && sizeStr != null) {
        Try {
          val size = sizeStr.toLong
          val modTime = if (modTimeStr != null) modTimeStr.toLong else System.currentTimeMillis()
          val dataChange = if (dataChangeStr != null) dataChangeStr.toBoolean else true
          
          // Parse partition values
          val partValues = extractPartitionValues(actionStr)
          
          AddFile(path, partValues, size, modTime, dataChange)
        }.toOption
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
  
  /**
   * Parse RemoveFile action from JSON string.
   */
  private def parseRemoveFile(actionStr: String): Option[RemoveFile] = {
    try {
      val path = extractJsonValue(actionStr, "path")
      val timestampStr = extractJsonValue(actionStr, "deletionTimestamp")
      val dataChangeStr = extractJsonValue(actionStr, "dataChange")
      
      if (path != null) {
        val timestamp = 
          if (timestampStr != null && timestampStr != "null") 
            Some(timestampStr.toLong) 
          else 
            None
            
        val dataChange = 
          if (dataChangeStr != null) 
            dataChangeStr.toBoolean 
          else 
            true
            
        Some(RemoveFile(path, timestamp, dataChange))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
  
  /**
   * Parse Metadata action from JSON string.
   */
  private def parseMetadata(json: String): Option[Metadata] = {
    try {
      val idStr = extractJsonValue(json, "id")
      val id = if (idStr != null) idStr else UUID.randomUUID().toString
      
      val name = extractJsonValue(json, "name")
      val description = extractJsonValue(json, "description")
      val schemaString = extractSchemaString(json)
      val partitionColumnsStr = extractJsonValue(json, "partitionColumns")
      val partitionColumns = if (partitionColumnsStr != null) {
        partitionColumnsStr.split(",").toSeq
      } else {
        Seq.empty[String]
      }
      
      // Create a map for configuration if it exists
      val configMap = extractJsonMapValues(json, "configuration")
      
      val createdTimeStr = extractJsonValue(json, "createdTime")
      val createdTime = if (createdTimeStr != null) createdTimeStr.toLong else System.currentTimeMillis()
      
      Some(Metadata(
        id = id,
        name = name,
        description = description,
        schemaString = schemaString,
        partitionColumns = partitionColumns,
        configuration = configMap,
        createdTime = createdTime
      ))
    } catch {
      case e: Exception => 
        println(s"Error parsing metadata: ${e.getMessage}, JSON: $json")
        e.printStackTrace()
        None
    }
  }
  
  /**
   * Extract the schema string specifically since it's a complex JSON object
   */
  private def extractSchemaString(json: String): String = {
    try {
      val schemaStartIndex = json.indexOf("\"schemaString\":")
      if (schemaStartIndex < 0) return null
      
      // Find the position after the key
      val contentStartIndex = schemaStartIndex + "\"schemaString\":".length
      val content = json.substring(contentStartIndex).trim
      
      // If it's null, return null
      if (content.startsWith("null")) return null
      
      // Handle properly quoted JSON object 
      if (content.startsWith("\"")) {
        // The schema is a quoted JSON string, find the closing quote
        var i = 1
        var escaped = false
        var found = false
        
        while (i < content.length && !found) {
          val c = content.charAt(i)
          if (c == '\\') {
            escaped = !escaped
          } else if (c == '"' && !escaped) {
            found = true
          } else {
            escaped = false
          }
          i += 1
        }
        
        if (found) {
          return content.substring(1, i - 1)  // Remove surrounding quotes
        }
      } else if (content.startsWith("{")) {
        // The schema is a direct JSON object, find the matching closing brace
        var depth = 1
        var i = 1
        
        while (i < content.length && depth > 0) {
          val c = content.charAt(i)
          if (c == '{') depth += 1
          else if (c == '}') depth -= 1
          i += 1
        }
        
        if (depth == 0) {
          val schemaString = content.substring(0, i)
          return schemaString
        }
      }
      
      // Fallback to using the normal extraction method 
      extractJsonValue(json, "schemaString")
    } catch {
      case e: Exception =>
        println(s"Error extracting schema string: ${e.getMessage}")
        null
    }
  }
  
  /**
   * Parse CommitInfo action from JSON string.
   */
  private def parseCommitInfo(actionStr: String): Option[CommitInfo] = {
    try {
      val timestamp = 
        Try(extractJsonValue(actionStr, "timestamp").toLong)
          .getOrElse(System.currentTimeMillis())
          
      val operation = extractJsonValue(actionStr, "operation")
      val opParamsMap = extractJsonMapValues(actionStr, "operationParameters")
      
      val readVersionStr = extractJsonValue(actionStr, "readVersion")
      val readVersion = 
        if (readVersionStr != null && readVersionStr != "null") 
          Try(readVersionStr.toLong).toOption 
        else 
          None
          
      val isBlindAppendStr = extractJsonValue(actionStr, "isBlindAppend")
      val isBlindAppend = 
        if (isBlindAppendStr != null && isBlindAppendStr != "null") 
          Try(isBlindAppendStr.toBoolean).toOption 
        else 
          None
          
      Some(CommitInfo(
        timestamp = timestamp,
        operation = operation,
        operationParameters = opParamsMap,
        readVersion = readVersion,
        isBlindAppend = isBlindAppend
      ))
    } catch {
      case e: Exception =>
        println(s"Error parsing commit info: ${e.getMessage}")
        None
    }
  }
  
  /**
   * Extract partition values from a JSON string.
   */
  private def extractPartitionValues(json: String): Map[String, String] = {
    val pattern = """\"partitionValues\"\s*:\s*\{([^}]*)\}""".r
    pattern.findFirstMatchIn(json).map { m =>
      val content = m.group(1)
      val keyValuePattern = """\"([^\"]+)\"\s*:\s*\"([^\"]*)\"""".r
      keyValuePattern.findAllMatchIn(content).map { kv =>
        kv.group(1) -> kv.group(2)
      }.toMap
    }.getOrElse(Map.empty)
  }
  
  /**
   * Extract values from a JSON array.
   */
  private def extractJsonArrayValues(json: String, key: String): Seq[String] = {
    val patternStr = "\"" + key + "\"\\s*:\\s*\\[([^\\]]*)\\]"
    val pattern = new Regex(patternStr)
    pattern.findFirstMatchIn(json).map { m =>
      val content = m.group(1)
      val valuePattern = "\"([^\"]*)\"".r
      valuePattern.findAllMatchIn(content).map(_.group(1)).toSeq
    }.getOrElse(Seq.empty)
  }
  
  /**
   * Extract values from a JSON map.
   */
  private def extractJsonMapValues(json: String, key: String): Map[String, String] = {
    val patternStr = "\"" + key + "\"\\s*:\\s*\\{([^}]*)\\}"
    val pattern = new Regex(patternStr)
    pattern.findFirstMatchIn(json).map { m =>
      val content = m.group(1)
      val keyValuePattern = "\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"".r
      keyValuePattern.findAllMatchIn(content).map { kv =>
        kv.group(1) -> kv.group(2)
      }.toMap
    }.getOrElse(Map.empty)
  }
  
  /**
   * Helper to extract values from JSON strings.
   */
  private def extractJsonValue(json: String, key: String): String = {
    try {
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
            null
          }
        } else if (json.contains("\"" + key + "\":null")) {
          // Explicit null value
          null
        } else {
          // Numeric, boolean, or object value
          val parts = json.split("\"" + key + "\":")
          val valuePart = parts.last.trim
          val endPos = Math.min(
            if (valuePart.indexOf(",") >= 0) valuePart.indexOf(",") else Int.MaxValue,
            if (valuePart.indexOf("}") >= 0) valuePart.indexOf("}") else Int.MaxValue
          )
          if (endPos < Int.MaxValue) {
            valuePart.substring(0, endPos).trim
          } else {
            valuePart.trim
          }
        }
      } else {
        null
      }
    } catch {
      case e: Exception => 
        println(s"Error extracting '$key' from JSON: ${e.getMessage}")
        null
    }
  }
  
  /**
   * List all available version files in the log.
   * @return Sequence of version numbers
   */
  def listVersions(): Seq[Long] = {
    val logDir = new File(logPath)
    if (!logDir.exists() || !logDir.isDirectory) {
      return Seq.empty
    }
    
    logDir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".json"))
      .map(f => f.getName.stripSuffix(".json").toLong)
      .sorted
  }
  
  /**
   * Read the contents of a version file.
   * @param version Version number
   * @return List of action strings
   */
  def readVersion(version: Long): Seq[String] = {
    val versionFile = new File(logPath, f"$version%020d.json")
    if (!versionFile.exists()) {
      return Seq.empty
    }
    
    val reader = new BufferedReader(new FileReader(versionFile))
    try {
      var line: String = null
      var closed = false
      var result = Seq.empty[String]
      
      while (!closed && {line = reader.readLine(); line != null}) {
        result = result :+ line
      }
      
      result
    } finally {
      reader.close()
    }
  }
  
  /**
   * Starts a new transaction on the table.
   * @return New transaction instance
   */
  def startTransaction(isolation: IsolationLevel = Serializable): OptimisticTransaction = {
    update()
    new OptimisticTransaction(this, isolation)
  }
  
  /**
   * Write a set of actions to a version file.
   * 
   * @param version Version number
   * @param actions Actions to write
   */
  def write(version: Long, actions: Seq[Action]): Unit = {
    logLock.lock()
    try {
      // Create log directory if it doesn't exist
      val logDir = new File(logPath)
      logDir.mkdirs()
      
      // Create version file
      val versionFile = new File(logDir, f"$version%020d.json")
      val writer = new BufferedWriter(new FileWriter(versionFile))
      try {
        // Write each action as a JSON line
        actions.foreach { action =>
          writer.write(action.json)
          writer.newLine()
        }
      } finally {
        writer.close()
      }
      
      // Force an update of the snapshot to reflect the new version
      _snapshot = null
      update()
      
      // Check if we need to create a checkpoint
      if (shouldCheckpoint(version)) {
        checkpoint(version)
      }
    } finally {
      logLock.unlock()
    }
  }
  
  /**
   * Read table metadata from the transaction log.
   */
  def readTableMetadata(): Option[Metadata] = {
    try {
      // Get the latest snapshot
      val snapshot = update()
      println(s"Reading metadata - snapshot version: ${snapshot.version}")
      
      // Get metadata actions from the log
      val versions = listVersions()
      println(s"Available versions: ${versions.mkString(", ")}")
      
      val metadataFiles = versions.flatMap { version =>
        println(s"Looking for metadata in version $version")
        val versionActions = readVersion(version)
        println(s"  Found ${versionActions.size} actions")
        
        versionActions.flatMap { actionStr =>
          println(s"  Action: ${actionStr.take(100)}...")
          if (actionStr.contains("\"metadata\"")) {
            println(s"  Found metadata action")
            parseMetadata(actionStr).map { m => 
              println(s"  Parsed metadata: ${m.name}")
              m
            }
          } else {
            println(s"  Not a metadata action")
            None
          }
        }
      }
      
      println(s"Found ${metadataFiles.size} metadata actions")
      
      // Return the latest metadata action if available
      if (metadataFiles.isEmpty) None else Some(metadataFiles.last)
    } catch {
      case e: Exception =>
        println(s"Error reading table metadata: ${e.getMessage}")
        e.printStackTrace()
        None
    }
  }
} 