package com.example.deltalite.actions

import java.util.UUID

/**
 * Base trait for all actions that can be written to the transaction log
 */
sealed trait Action extends Serializable {
  /** Convert the action to a JSON string */
  def json: String
}

/**
 * Protocol defines the min reader and writer versions
 */
case class Protocol(
    minReaderVersion: Int = 1,
    minWriterVersion: Int = 1,
    readerFeatures: Seq[String] = Seq.empty,
    writerFeatures: Seq[String] = Seq.empty
) extends Action {
  override def json: String = {
    val readerFeaturesJson = if (readerFeatures.isEmpty) "" else {
      val featuresStr = readerFeatures.map(f => "\"" + f + "\"").mkString(", ")
      s""", "readerFeatures": [$featuresStr]"""
    }
    
    val writerFeaturesJson = if (writerFeatures.isEmpty) "" else {
      val featuresStr = writerFeatures.map(f => "\"" + f + "\"").mkString(", ")
      s""", "writerFeatures": [$featuresStr]"""
    }
    
    s"""{"protocol":{"minReaderVersion":$minReaderVersion,"minWriterVersion":$minWriterVersion$readerFeaturesJson$writerFeaturesJson}}"""
  }
}

/**
 * Information about a commit
 */
case class CommitInfo(
    timestamp: Long = System.currentTimeMillis(),
    operation: String = null,
    operationParameters: Map[String, String] = Map.empty,
    readVersion: Option[Long] = None,
    isBlindAppend: Option[Boolean] = None,
    isolationLevel: String = null,
    operationMetrics: Map[String, String] = Map.empty,
    userMetadata: String = null
) extends Action {
  override def json: String = {
    val readVersionJson = readVersion.map(v => s"$v").getOrElse("null")
    val isBlindAppendJson = isBlindAppend.map(b => s"$b").getOrElse("null")
    val operationJson = if (operation == null) "null" else s""""$operation""""
    val isolationJson = if (isolationLevel == null) "" else s""","isolationLevel":"$isolationLevel""""
    val opParamsJson = operationParameters.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    val opMetricsJson = operationMetrics.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    val userMetadataJson = if (userMetadata == null) "" else s""","userMetadata":"$userMetadata""""
    
    s"""{"commitInfo":{"timestamp":$timestamp,"operation":$operationJson,"operationParameters":$opParamsJson,"readVersion":$readVersionJson,"isBlindAppend":$isBlindAppendJson$isolationJson,"operationMetrics":$opMetricsJson$userMetadataJson}}"""
  }
}

/**
 * Represents a file addition to the table
 */
case class AddFile(
    path: String,                          // Relative path within the table directory
    partitionValues: Map[String, String] = Map.empty,  // Partition column values
    size: Long,                            // File size in bytes
    modificationTime: Long = System.currentTimeMillis(), // Last modification timestamp
    dataChange: Boolean = true,            // Whether this file contains new data
    tags: Map[String, String] = Map.empty  // Additional metadata tags
) extends Action {
  override def json: String = {
    val partValuesJson = partitionValues.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    val tagsJson = tags.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    
    s"""{"add":{"path":"$path","partitionValues":$partValuesJson,"size":$size,"modificationTime":$modificationTime,"dataChange":$dataChange,"tags":$tagsJson}}"""
  }
}

/**
 * Represents a file removal from the table
 */
case class RemoveFile(
    path: String,                          // Path of the file to remove
    deletionTimestamp: Option[Long] = None, // When the file was deleted
    dataChange: Boolean = true             // Whether this represents data being deleted
) extends Action {
  override def json: String = {
    val timestamp = deletionTimestamp.map(t => s"$t").getOrElse("null")
    
    s"""{"remove":{"path":"$path","deletionTimestamp":$timestamp,"dataChange":$dataChange}}"""
  }
}

/**
 * Represents table metadata
 */
case class Metadata(
    id: String = UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    createdTime: Long = System.currentTimeMillis()
) extends Action {
  override def json: String = {
    val nameJson = if (name == null) "null" else s""""$name""""
    val descJson = if (description == null) "null" else s""""$description""""
    val schemaJson = if (schemaString == null) "null" else s"""$schemaString"""
    val partColsJson = partitionColumns.map(c => s""""$c"""").mkString("[", ",", "]")
    val configJson = configuration.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    
    // Explicitly order fields in the JSON to ensure consistent parsing
    s"""{"metadata":{"id":"$id","name":$nameJson,"description":$descJson,"schemaString":$schemaJson,"partitionColumns":$partColsJson,"configuration":$configJson,"createdTime":$createdTime}}"""
  }
} 