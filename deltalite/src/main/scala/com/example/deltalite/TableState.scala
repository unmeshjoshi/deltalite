package com.example.deltalite

import com.example.deltalite.actions._
import com.example.deltalite.io.SimpleFileFormat
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MutableMap}

/**
 * Represents the state of a table at a specific version.
 * This is a more structured representation than just storing raw action JSON.
 */
class TableState(val version: Long) extends Serializable {
  
  // Valid files in the state (files that have been added but not removed)
  private val _files = new ArrayBuffer[AddFile]()
  
  // Table metadata
  private var _metadata: Metadata = _
  
  // Latest commit info
  private var _commitInfo: CommitInfo = _
  
  /**
   * Get all valid files (files that have been added but not removed)
   */
  def files: Seq[AddFile] = _files.toSeq
  
  /**
   * Get table metadata
   */
  def metadata: Option[Metadata] = Option(_metadata)
  
  /**
   * Get latest commit info
   */
  def commitInfo: Option[CommitInfo] = Option(_commitInfo)
  
  /**
   * Apply an action to update the state
   */
  def applyAction(action: Action): Unit = {
    action match {
      case addFile: AddFile =>
        _files += addFile
      
      case removeFile: RemoveFile =>
        // Use filter instead of filterInPlace for Scala 2.12 compatibility
        val filteredFiles = _files.filter(f => f.path != removeFile.path)
        _files.clear()
        _files ++= filteredFiles
      
      case metadata: Metadata =>
        _metadata = metadata
      
      case commitInfo: CommitInfo =>
        _commitInfo = commitInfo
      
      case _ => // Ignore unknown actions
    }
  }
  
  /**
   * Apply a sequence of actions to update the state
   */
  def applyActions(actions: Seq[Action]): Unit = {
    actions.foreach(applyAction)
  }
  
  /**
   * Read all records from the files in this state
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @return Sequence of records
   */
  def readRecords[T](converter: Map[String, String] => T): Seq[T] = {
    // Process one file at a time and track records by a key to prevent duplicates
    // We'll use the record's "id" field as the key, assuming it exists
    val recordsById = MutableMap[String, T]()
    
    _files.foreach { file =>
      val records = SimpleFileFormat.readRecords(file.path)
      // Use record's ID (if available) to prevent duplicates
      records.foreach { record =>
        val converted = converter(record)
        // Try to use id field as key if available, otherwise use the record's toString
        val key = record.getOrElse("id", record.toString)
        recordsById.put(key, converted)
      }
    }
    
    // Return the deduplicated records as a List
    recordsById.values.toList
  }
  
  /**
   * Find records matching a predicate
   * 
   * @tparam T The type to convert records to
   * @param converter Function to convert from Map[String, String] to T
   * @param predicate Function to filter records
   * @return Sequence of matching records
   */
  def findRecords[T](converter: Map[String, String] => T, predicate: T => Boolean): Seq[T] = {
    readRecords(converter).filter(predicate)
  }
} 