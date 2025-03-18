package com.example.deltalite

import com.example.deltalite.actions._
import com.example.deltalite.io.SimpleFileFormat
import scala.collection.mutable.{ArrayBuffer, HashSet}
import java.io.File
import java.util.UUID
import scala.util.{Try, Success, Failure}

/**
 * Represents the isolation level for a transaction.
 */
sealed trait IsolationLevel
case object Serializable extends IsolationLevel
case object WriteSerializable extends IsolationLevel
case object SnapshotIsolation extends IsolationLevel

/**
 * Handles transaction management with optimistic concurrency control.
 */
class OptimisticTransaction(
    val deltaLog: DeltaLiteLog, 
    val isolationLevel: IsolationLevel = Serializable,
    val maxRetries: Int = 3
) {
  
  // The snapshot this transaction is based on
  private var _snapshot: Snapshot = deltaLog.snapshot
  
  // Actions to commit
  private val actions = new ArrayBuffer[Action]()
  
  // Files read by this transaction (for conflict detection)
  private val readFiles = new HashSet[String]()
  
  // Files written by this transaction (for conflict detection)
  private val writtenFiles = new HashSet[String]()
  
  // Retry count
  private var retryCount = 0
  
  /**
   * The snapshot this transaction is based on.
   */
  def snapshot: Snapshot = _snapshot
  
  /**
   * Read version for this transaction.
   */
  def readVersion: Long = _snapshot.version
  
  /**
   * Records that we read a file (for conflict detection).
   */
  def readFile(path: String): Unit = {
    readFiles += path
  }
  
  /**
   * Records that we read multiple files.
   */
  def readFiles(paths: Seq[String]): Unit = {
    readFiles ++= paths
  }
  
  /**
   * Adds an action to the transaction.
   */
  def addAction(action: Action): Unit = {
    actions += action
    
    // Track written files for conflict detection
    action match {
      case addFile: AddFile => 
        writtenFiles += addFile.path
      case removeFile: RemoveFile => 
        writtenFiles += removeFile.path
      case _ => // Other actions don't affect files
    }
  }
  
  /**
   * Adds multiple actions to the transaction.
   */
  def addActions(newActions: Seq[Action]): Unit = {
    newActions.foreach(addAction)
  }
  
  /**
   * Writes records to a new file.
   *
   * @param records Records to write
   * @param partitionValues Partition values for these records
   * @return AddFile action for the written file
   */
  def writeFile(records: Seq[Map[String, String]], partitionValues: Map[String, String] = Map.empty): AddFile = {
    // Generate a unique file name
    val fileId = UUID.randomUUID().toString
    
    // Create path based on partition values
    val partitionPath = if (partitionValues.isEmpty) {
      ""
    } else {
      partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
    }
    
    val filePath = s"${deltaLog.tablePath}/$partitionPath$fileId.csv"
    
    // Write the records to a file
    val fileSize = SimpleFileFormat.writeRecords(records, filePath)
    
    // Create AddFile action
    val addFile = AddFile(
      path = filePath,
      partitionValues = partitionValues,
      size = fileSize,
      modificationTime = System.currentTimeMillis()
    )
    
    // Add to transaction actions
    addAction(addFile)
    
    addFile
  }
  
  /**
   * Writes multiple files, one for each partition.
   *
   * @param recordsByPartition Map of partition values to records for that partition
   * @return Sequence of AddFile actions
   */
  def writeFiles(recordsByPartition: Map[Map[String, String], Seq[Map[String, String]]]): Seq[AddFile] = {
    recordsByPartition.map { case (partitionValues, records) =>
      writeFile(records, partitionValues)
    }.toSeq
  }
  
  /**
   * Commits the transaction with the current set of actions.
   *
   * @param operation Name of the operation
   * @return Version that was committed
   * @throws ConcurrentModificationException if a conflict is detected and max retries exceeded
   */
  def commit(operation: String): Long = {
    // Force update of the DeltaLog to get latest snapshot before checking conflicts
    deltaLog.update()
    
    // Check for conflicts
    checkForConflicts()
    
    // Create commit info
    val commitInfo = CommitInfo(
      timestamp = System.currentTimeMillis(),
      operation = operation,
      readVersion = Some(readVersion),
      isBlindAppend = Some(isBlindAppend)
    )
    
    // Add commit info to actions
    val finalActions = commitInfo +: actions
    
    // Calculate next version
    val nextVersion = if (readVersion == 0) {
      // Special handling for version 0
      val versions = deltaLog.listVersions()
      if (versions.isEmpty) {
        // First commit ever - use version 0
        0
      } else if (versions.max == 0) {
        // Version 0 exists, but we're continuing to work on it (operations in sequence)
        // Use version 1 for the next commit
        1
      } else {
        // Otherwise use the next version after the current max
        versions.max + 1
      }
    } else {
      // Normal case - increment from the read version
      readVersion + 1
    }
    
    // Write to the log
    deltaLog.write(nextVersion, finalActions)
    
    // Return the new version
    nextVersion
  }
  
  /**
   * Commits the transaction with automatic retry logic.
   *
   * @param operation Name of the operation
   * @return Version that was committed
   */
  def commitWithRetry(operation: String): Long = {
    try {
      commit(operation)
    } catch {
      case e: ConcurrentModificationException if retryCount < maxRetries =>
        // Retry the transaction
        retryCount += 1
        println(s"Retrying transaction (attempt ${retryCount}): ${e.getMessage}")
        
        // Update snapshot and retry
        _snapshot = deltaLog.update()
        
        // Clear previous actions (but keep read files for proper conflict detection)
        actions.clear()
        writtenFiles.clear()
        
        // Retry
        commitWithRetry(operation)
        
      case e: ConcurrentModificationException =>
        // Max retries exceeded
        throw new ConcurrentModificationException(
          s"Transaction failed after $maxRetries retries: ${e.getMessage}", e)
    }
  }
  
  /**
   * Commits a set of actions to the transaction log.
   *
   * @param newActions Actions to commit
   * @param operation Name of the operation
   * @return Version that was committed
   * @throws ConcurrentModificationException if a conflict is detected
   */
  def commit(newActions: Seq[Action], operation: String): Long = {
    // Add the actions to the transaction
    addActions(newActions)
    
    // Commit
    commit(operation)
  }
  
  /**
   * Check if this transaction is a blind append (no existing files read or modified).
   */
  private def isBlindAppend: Boolean = {
    readFiles.isEmpty && actions.collect { 
      case r: RemoveFile => r 
    }.isEmpty
  }
  
  /**
   * Check for conflicts with concurrent transactions.
   * 
   * @throws ConcurrentModificationException if a conflict is detected
   */
  private def checkForConflicts(): Unit = {
    // Get the latest snapshot
    val latestSnapshot = deltaLog.update()
    
    // If the latest version is different from our read version, we need to check for conflicts
    if (latestSnapshot.version != readVersion) {
      // Get actions added since our read version
      val newVersions = (readVersion + 1) to latestSnapshot.version
      
      // Check for conflicts based on isolation level
      isolationLevel match {
        case Serializable =>
          // Any concurrent change is a conflict except when it's the same transaction
          throw new ConcurrentModificationException(
            s"Concurrent transaction conflict: read version was $readVersion but current version is ${latestSnapshot.version}")
          
        case WriteSerializable =>
          // Check if any files we read were modified
          val modifiedFiles = newVersions.flatMap { version =>
            deltaLog.readVersion(version).flatMap { actionStr =>
              if (actionStr.contains("\"add\"") || actionStr.contains("\"remove\"")) {
                val path = extractJsonValue(actionStr, "path")
                Option(path)
              } else {
                None
              }
            }
          }.toSet
          
          val conflictingFiles = readFiles.intersect(modifiedFiles)
          if (conflictingFiles.nonEmpty) {
            throw new ConcurrentModificationException(
              s"Concurrent transaction conflict: files we read were modified by another transaction: ${conflictingFiles.mkString(", ")}")
          }
          
        case SnapshotIsolation =>
          // Check if any files we're writing to were modified
          val modifiedFiles = newVersions.flatMap { version =>
            deltaLog.readVersion(version).flatMap { actionStr =>
              if (actionStr.contains("\"add\"") || actionStr.contains("\"remove\"")) {
                val path = extractJsonValue(actionStr, "path")
                Option(path)
              } else {
                None
              }
            }
          }.toSet
          
          val conflictingFiles = writtenFiles.intersect(modifiedFiles)
          if (conflictingFiles.nonEmpty) {
            throw new ConcurrentModificationException(
              s"Concurrent transaction conflict: files we're writing were modified by another transaction: ${conflictingFiles.mkString(", ")}")
          }
      }
    }
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
    } catch {
      case _: Exception => null
    }
  }
}

/**
 * Exception thrown when a conflict is detected.
 */
class ConcurrentModificationException(message: String, cause: Throwable = null) 
  extends RuntimeException(message, cause) 