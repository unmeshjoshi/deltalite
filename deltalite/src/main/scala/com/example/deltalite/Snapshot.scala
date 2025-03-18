package com.example.deltalite

import com.example.deltalite.actions._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import scala.util.Try

/**
 * Represents an immutable snapshot of the table at a specific version.
 */
class Snapshot(val version: Long, val deltaLog: DeltaLiteLog, private val prebuiltState: Option[TableState] = None) {
  
  /**
   * The state of the table reconstructed from the transaction log.
   */
  lazy val state: TableState = {
    // If we have a prebuilt state from a checkpoint, use it
    prebuiltState.getOrElse {
      val tableState = new TableState(version)
      
      if (version >= 0) {
        // Get all versions up to and including this version
        val versions = deltaLog.listVersions().filter(_ <= version)
        
        // Parse all actions and apply them to the state
        for {
          ver <- versions
          actionStr <- deltaLog.readVersion(ver)
          action <- parseAction(actionStr)
        } {
          tableState.applyAction(action)
        }
      }
      
      tableState
    }
  }
  
  /**
   * All valid files in this snapshot.
   */
  def allFiles: Seq[AddFile] = state.files
  
  /**
   * Table metadata in this snapshot.
   */
  def metadata: Option[Metadata] = state.metadata
  
  /**
   * Latest commit info.
   */
  def commitInfo: Option[CommitInfo] = state.commitInfo
  
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
  private def parseMetadata(actionStr: String): Option[Metadata] = {
    try {
      // Extract fields
      val id = extractJsonValue(actionStr, "id")
      val name = extractJsonValue(actionStr, "name")
      val description = extractJsonValue(actionStr, "description")
      
      // Handle schemaString specially since it's a complex JSON structure
      val schemaStringStart = actionStr.indexOf("\"schemaString\":")
      var schemaString: String = null
      
      if (schemaStringStart >= 0) {
        val contentStart = schemaStringStart + "\"schemaString\":".length
        var depth = 0
        var inString = false
        var escape = false
        var i = contentStart
        
        // Skip leading whitespace and find the first {
        while (i < actionStr.length && actionStr.charAt(i) != '{') {
          i += 1
        }
        
        if (i < actionStr.length && actionStr.charAt(i) == '{') {
          val start = i
          depth = 1
          
          // Parse until we find the matching closing bracket
          i += 1
          while (i < actionStr.length && depth > 0) {
            val c = actionStr.charAt(i)
            
            if (escape) {
              escape = false
            } else if (c == '\\') {
              escape = true
            } else if (c == '"' && !escape) {
              inString = !inString
            } else if (!inString) {
              if (c == '{') depth += 1
              else if (c == '}') depth -= 1
            }
            
            i += 1
          }
          
          if (depth == 0) {
            // Successfully parsed the full schema JSON
            schemaString = actionStr.substring(start, i)
          }
        }
      }
      
      // Get other fields
      val partitionColumns = extractJsonArrayValues(actionStr, "partitionColumns")
      val configuration = extractJsonMapValues(actionStr, "configuration")
      val createdTimeStr = extractJsonValue(actionStr, "createdTime")
      val createdTime = if (createdTimeStr != null) createdTimeStr.toLong else System.currentTimeMillis()
      
      Some(Metadata(
        id = if (id != null) id else java.util.UUID.randomUUID().toString,
        name = if (name != null && name != "null") name else null,
        description = if (description != null && description != "null") description else null,
        schemaString = schemaString,
        partitionColumns = partitionColumns,
        configuration = configuration,
        createdTime = createdTime
      ))
    } catch {
      case e: Exception => 
        // Log or handle parsing error
        println(s"Error parsing metadata action: ${e.getMessage}")
        None
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
      null
    }
  }
} 