package com.example.deltalite.examples

import com.example.deltalite._
import com.example.deltalite.model.Customer
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Example demonstrating time travel functionality in DeltaLite.
 * 
 * This example shows how to:
 * 1. Create a table with some initial data
 * 2. Modify the data over multiple versions
 * 3. Use time travel to read the table at different points in history
 */
object TimeTravelExample {
  
  def main(args: Array[String]): Unit = {
    // Create a table in a temporary directory
    val tempDir = System.getProperty("java.io.tmpdir")
    val tablePath = new File(tempDir, s"timetravel_example_${System.currentTimeMillis()}").getAbsolutePath
    
    println(s"Creating table at: $tablePath")
    
    // Create the table object
    val table = new DeltaLiteTable(tablePath)
    
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
      name = "timetravel_demo",
      schemaJson = schema
    )
    println(s"Table created at version $v0")
    
    // Helper function to print table contents
    def printTableContents[T](version: Option[Long] = None): Unit = {
      val customers = version match {
        case Some(v) => table.timeTravel(v, Customer.fromMap)
        case None => table.readAll(Customer.fromMap)
      }
      
      val versionString = version.map(v => s"Version $v").getOrElse("Current version")
      println(s"Table Contents ($versionString):")
      if (customers.isEmpty) {
        println("  Empty table")
      } else {
        customers.foreach(c => println(s"  ${c.id}, ${c.name}, ${c.email}"))
      }
      println()
    }
    
    // Initial batch of data - Version 1
    println("Inserting initial batch (Version 1)")
    val initialCustomers = (1 to 5).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v1 = table.insert(initialCustomers)
    println(s"Initial data inserted at version $v1")
    printTableContents()
    
    // Update some records - Version 2
    println("Updating records (Version 2)")
    val v2 = table.update[Customer](
      converter = Customer.fromMap,
      recordToMap = _.toMap,
      condition = _.id <= 3,
      update = c => Customer(c.id, s"Updated ${c.name}", c.email)
    )
    println(s"Updated records at version $v2")
    printTableContents()
    
    // Delete some records - Version 3
    println("Deleting records (Version 3)")
    val v3 = table.delete[Customer](
      converter = Customer.fromMap,
      recordToMap = _.toMap,
      condition = _.id == 2
    )
    println(s"Deleted record at version $v3")
    printTableContents()
    
    // Add more records - Version 4
    println("Adding more records (Version 4)")
    val newCustomers = (6 to 10).map(i => 
      Customer(i, s"Customer $i", s"customer$i@example.com").toMap
    )
    val v4 = table.insert(newCustomers)
    println(s"Added new records at version $v4")
    printTableContents()
    
    // Now demonstrate time travel
    println("====== Time Travel Demonstration ======")
    
    // Read at version 1 (initial data)
    println("Time travel to version 1 (initial data):")
    printTableContents(Some(1))
    
    // Read at version 2 (after updates)
    println("Time travel to version 2 (after updates):")
    printTableContents(Some(2))
    
    // Read at version 3 (after deletion)
    println("Time travel to version 3 (after deletion):")
    printTableContents(Some(3))
    
    // Read current version for comparison
    println("Current version (for comparison):")
    printTableContents()
    
    // Demonstrate timeTravelAndFind with a predicate
    println("\nFinding specific records in version 1:")
    val customersV1 = table.timeTravelAndFind(
      version = 1,
      converter = Customer.fromMap,
      predicate = (c: Customer) => c.id > 3
    )
    customersV1.foreach(c => println(s"  ${c.id}, ${c.name}, ${c.email}"))
    
    // List all available versions
    val versions = table.deltaLog.listVersions().sorted
    println(s"\nAll available versions: ${versions.mkString(", ")}")
    
    println("\nTime travel example completed successfully!")
  }
} 