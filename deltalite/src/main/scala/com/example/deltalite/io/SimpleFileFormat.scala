package com.example.deltalite.io

import java.io.{File, FileWriter, BufferedWriter, FileReader, BufferedReader}
import scala.collection.mutable.ArrayBuffer

/**
 * Simplified file format that can read and write records.
 * We use a very basic CSV format to focus on transaction mechanics.
 */
object SimpleFileFormat {
  /**
   * Reads records from a CSV file
   *
   * @param path Path to the file
   * @return List of records as Maps with column name -> value
   */
  def readRecords(path: String): Seq[Map[String, String]] = {
    val file = new File(path)
    if (!file.exists()) {
      return Seq.empty
    }
    
    val reader = new BufferedReader(new FileReader(file))
    try {
      // Read all lines at once into a strict collection (not a lazy stream)
      val lines = Iterator.continually(reader.readLine())
                         .takeWhile(_ != null)
                         .toList  // Convert to strict collection
      
      if (lines.isEmpty) {
        return Seq.empty
      }
      
      // Parse header and records
      val header = lines.head.split(",").map(_.trim)
      lines.tail.map { line =>
        val values = line.split(",").map(_.trim)
        header.zip(values).toMap
      }
    } finally {
      reader.close()
    }
  }
  
  /**
   * Writes records to a CSV file
   *
   * @param records Records to write as Maps with column name -> value
   * @param path Path where to write the file
   * @return Size of the written file in bytes
   */
  def writeRecords(records: Seq[Map[String, String]], path: String): Long = {
    if (records.isEmpty) {
      return 0L
    }
    
    // Create parent directories if they don't exist
    val file = new File(path)
    file.getParentFile.mkdirs()
    
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      // Write header (using keys from the first record)
      val header = records.head.keys.toSeq
      writer.write(header.mkString(","))
      writer.newLine()
      
      // Write records
      records.foreach { record =>
        val values = header.map(h => record.getOrElse(h, "null"))
        writer.write(values.mkString(","))
        writer.newLine()
      }
    } finally {
      writer.close()
    }
    
    // Return file size
    file.length()
  }
} 