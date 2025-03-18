package com.example.deltalite.examples

import com.example.deltalite._
import com.example.deltalite.model.Customer
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Example demonstrating checkpoint functionality in DeltaLite.
 * 
 * Checkpoints are used to optimize table state loading by persisting a snapshot
 * of the table state at specific version intervals. This avoids having to replay 
 * the entire transaction log when loading a table with a long history.
 */
object CheckpointExample {
  
  def main(args: Array[String]): Unit = {
    // Create a table in a temporary directory
    val tempDir = System.getProperty("java.io.tmpdir")
    val tablePath = new File(tempDir, s"checkpoint_example_${System.currentTimeMillis()}").getAbsolutePath
    
    println(s"Creating table at: $tablePath")
    
    // Create the table object with a low checkpoint interval (for demonstration)
    val table = DeltaLiteTable.forPath(tablePath)
    
    // Set checkpoint interval to 3 versions (default is 10)
    // This means a checkpoint will be created every 3 versions
    table.deltaLog.checkpointInterval = 3
    
    // Define schema
    val schema = """
      {
        "type": "struct",
        "fields": [
          {"name": "id", "type": "integer", "nullable": false},
          {"name": "name", "type": "string", "nullable": false},
          {"name": "email", "type": "string", "nullable": true}
        ]
      }
    """
    
    // Create the table - Version 0
    println("Creating table (Version 0)")
    val v0 = table.createTable(
      name = "checkpoint_demo",
      schemaJson = schema
    )
    println(s"Table created at version $v0")
    
    // Function to check if checkpoint exists
    def checkpointExists(version: Long): Boolean = {
      val checkpointPath = s"$tablePath/_delta_log/_checkpoints/$version.checkpoint"
      Files.exists(Paths.get(checkpointPath))
    }
    
    // Function to print checkpoint status
    def printCheckpointStatus(): Unit = {
      val checkpointDir = new File(s"$tablePath/_delta_log/_checkpoints")
      if (checkpointDir.exists()) {
        val checkpoints = checkpointDir.listFiles()
          .filter(f => f.isFile && f.getName.endsWith(".checkpoint"))
          .map(f => f.getName.stripSuffix(".checkpoint").toLong)
          .sorted
        
        println(s"Checkpoints: ${checkpoints.mkString(", ")}")
      } else {
        println("No checkpoints created yet")
      }
    }
    
    printCheckpointStatus()
    
    // Write some data - Version 1
    println("\nInserting batch 1 (Version 1)")
    val batch1 = (1 to 5).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v1 = table.insert(batch1)
    println(s"Inserted batch 1 at version $v1")
    printCheckpointStatus() // No checkpoint yet (v1 % 3 != 0)
    
    // Write more data - Version 2
    println("\nInserting batch 2 (Version 2)")
    val batch2 = (6 to 10).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v2 = table.insert(batch2)
    println(s"Inserted batch 2 at version $v2")
    printCheckpointStatus() // No checkpoint yet (v2 % 3 != 0)
    
    // Write more data - Version 3
    println("\nInserting batch 3 (Version 3)")
    val batch3 = (11 to 15).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v3 = table.insert(batch3)
    println(s"Inserted batch 3 at version $v3")
    println("Checkpoint should be created for version 3 (v3 % 3 = 0)")
    printCheckpointStatus() // Checkpoint created (v3 % 3 = 0)
    
    // Force checkpoint (for demonstration)
    if (!checkpointExists(v3)) {
      println("\nManually creating checkpoint for version 3")
      table.deltaLog.checkpoint(v3)
      printCheckpointStatus()
    }
    
    // Simulate table reload to show faster loading from checkpoint
    println("\nSimulating table reload to demonstrate checkpoint loading")
    
    // Measure loading time without checkpoint
    val startNoCheckpoint = System.nanoTime()
    val snapshotNoCheckpoint = new Snapshot(v3, table.deltaLog)
    val timeNoCheckpoint = (System.nanoTime() - startNoCheckpoint) / 1000000.0
    println(f"Loading version $v3 without using checkpoint: $timeNoCheckpoint%.2f ms")
    
    // Measure loading time with checkpoint
    val startWithCheckpoint = System.nanoTime()
    val reloadedTable = DeltaLiteTable.forPath(tablePath)
    val timeWithCheckpoint = (System.nanoTime() - startWithCheckpoint) / 1000000.0
    println(f"Loading version $v3 using checkpoint: $timeWithCheckpoint%.2f ms")
    
    // Perform a few more operations to create additional checkpoints
    println("\nPerforming more operations")
    
    // Update - Version 4
    println("Updating records (Version 4)")
    val v4 = table.update[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      recordToMap = (c: Customer) => c.toMap,
      condition = (c: Customer) => c.id <= 5,
      update = (c: Customer) => Customer(c.id, s"Updated ${c.name}", c.email)
    )
    println(s"Updated records at version $v4")
    printCheckpointStatus() // No new checkpoint (v4 % 3 != 0)
    
    // Delete some records - Version 5
    println("\nDeleting records (Version 5)")
    val v5 = table.delete[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      recordToMap = (c: Customer) => c.toMap,
      condition = (c: Customer) => c.id > 10
    )
    println(s"Deleted records at version $v5")
    printCheckpointStatus() // No new checkpoint (v5 % 3 != 0)
    
    // Insert more records - Version 6
    println("\nInserting batch 4 (Version 6)")
    val batch4 = (16 to 20).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v6 = table.insert(batch4)
    println(s"Inserted batch 4 at version $v6")
    println("Checkpoint should be created for version 6 (v6 % 3 = 0)")
    printCheckpointStatus() // Checkpoint created (v6 % 3 = 0)
    
    // Demonstrate history
    println("\nTable history:")
    table.history().foreach { case (version, operation, timestamp) =>
      val hasCheckpoint = checkpointExists(version)
      val checkpointStr = if (hasCheckpoint) "[CHECKPOINT]" else ""
      println(f"Version $version: $operation at ${new java.util.Date(timestamp)} $checkpointStr")
    }
    
    // Count records
    val finalCount = table.readAll(map => map("id").toInt).size
    println(s"\nFinal record count: $finalCount")
    
    println("\nCheckpoint example complete")
    
    // Print table path for reference
    println(s"Table path: $tablePath")
    println("You can examine the files in this directory to see the transaction log and checkpoints")
  }
} 