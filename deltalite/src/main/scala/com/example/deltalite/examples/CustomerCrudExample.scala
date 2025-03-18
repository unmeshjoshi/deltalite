package com.example.deltalite.examples

import com.example.deltalite._
import com.example.deltalite.actions._
import com.example.deltalite.model.Customer
import com.example.deltalite.io.SimpleFileFormat
import java.io.File

/**
 * Complete example showing CRUD operations for Customer data using DeltaLite.
 */
object CustomerCrudExample {
  def main(args: Array[String]): Unit = {
    // Create a temporary directory for our table
    val tempDir = System.getProperty("java.io.tmpdir")
    val tablePath = s"$tempDir/customer_table_${System.currentTimeMillis()}"
    
    println(s"Creating table at: $tablePath")
    
    // Initialize DeltaLiteTable
    val table = DeltaLiteTable.forPath(tablePath)
    
    // CREATE: Create the customer table
    val createVersion = createCustomerTable(table)
    println(s"Created customer table at version $createVersion")
    
    // READ: Verify table was created
    val initialMetadata = readTableMetadata(table)
    println(s"Table name: ${initialMetadata.map(_.name).getOrElse("unknown")}")
    println(s"Table schema: ${initialMetadata.map(_.schemaString).getOrElse("unknown")}")
    
    // CREATE: Insert initial customers
    val insertedVersion = insertCustomers(table, Seq(
      Customer(1, "John Doe", "john@example.com"),
      Customer(2, "Jane Doe", "jane@example.com"),
      Customer(3, "Bob Smith", "bob@example.com"),
      Customer(4, "Alice Johnson", "alice@example.com")
    ))
    println(s"Inserted customers at version $insertedVersion")
    
    // READ: Read all customers
    val initialCustomers = readAllCustomers(table)
    println(s"Read ${initialCustomers.size} customers:")
    initialCustomers.foreach(c => println(s"  ${c.id}: ${c.name} (${c.email})"))
    
    // UPDATE: Update a customer
    val updatedVersion = updateCustomer(table, 2, "Jane Updated", "jane.updated@example.com")
    println(s"Updated customer at version $updatedVersion")
    
    // READ: Read customer by ID
    val customer2 = findCustomerById(table, 2)
    println(s"Customer 2: ${customer2.map(c => s"${c.name} (${c.email})").getOrElse("not found")}")
    
    // DELETE: Delete a customer
    val deletedVersion = deleteCustomer(table, 3)
    println(s"Deleted customer at version $deletedVersion")
    
    // READ: Get final customer list
    val finalCustomers = readAllCustomers(table)
    println(s"Final customer list (${finalCustomers.size}):")
    finalCustomers.foreach(c => println(s"  ${c.id}: ${c.name} (${c.email})"))
    
    // Transaction history
    val history = table.history()
    println(s"Transaction history (${history.size} versions):")
    history.foreach { case (version, operation, timestamp) =>
      println(s"  Version $version: $operation at ${new java.util.Date(timestamp)}")
    }
  }
  
  /**
   * Create a customer table with schema
   */
  def createCustomerTable(deltaLog: DeltaLiteLog): Long = {
    val txn = deltaLog.startTransaction()
    
    // Define customer schema as a JSON string - use proper format
    val schemaString = """
      {
        "type": "struct",
        "fields": [
          {"name": "id", "type": "integer", "nullable": false},
          {"name": "name", "type": "string", "nullable": false},
          {"name": "email", "type": "string", "nullable": true}
        ]
      }
    """
    
    // Create metadata action - use fully qualified class name to ensure the right one is used
    val metadata = new com.example.deltalite.actions.Metadata(
      id = java.util.UUID.randomUUID().toString,
      name = "customers",
      description = "Customer information",
      schemaString = schemaString,
      partitionColumns = Seq.empty,
      configuration = Map("deltalite.version" -> "1.0"),
      createdTime = System.currentTimeMillis()
    )
    
    println(s"Creating metadata: ${metadata.json}")
    
    // Commit the metadata
    val version = txn.commit(Seq(metadata), "CREATE TABLE")
    
    // Verify if metadata was written correctly
    val readMetadata = deltaLog.readTableMetadata()
    println(s"Read metadata: ${readMetadata}")
    
    version
  }
  
  /**
   * Create a customer table with schema using DeltaLiteTable
   */
  def createCustomerTable(table: DeltaLiteTable): Long = {
    // Get the underlying deltaLog
    val deltaLog = table.deltaLog
    
    // Call the method that works directly with DeltaLiteLog
    createCustomerTable(deltaLog)
  }
  
  /**
   * Insert customers into the table
   */
  def insertCustomers(table: DeltaLiteTable, customers: Seq[Customer]): Long = {
    // Convert to map representation for storage
    val records = customers.map(_.toMap)
    
    // Insert records
    table.insert(records)
  }
  
  // Overload for backward compatibility with tests
  def insertCustomers(deltaLog: DeltaLiteLog, customers: Seq[Customer]): Long = {
    insertCustomers(new DeltaLiteTable(deltaLog.tablePath), customers)
  }
  
  /**
   * Read all customers from the table
   */
  def readAllCustomers(table: DeltaLiteTable): Seq[Customer] = {
    table.readAll(Customer.fromMap)
  }
  
  // Overload for backward compatibility with tests
  def readAllCustomers(deltaLog: DeltaLiteLog): Seq[Customer] = {
    readAllCustomers(new DeltaLiteTable(deltaLog.tablePath))
  }
  
  /**
   * Find a customer by ID
   */
  def findCustomerById(table: DeltaLiteTable, id: Int): Option[Customer] = {
    table.find(Customer.fromMap, (c: Customer) => c.id == id).headOption
  }
  
  // Overload for backward compatibility with tests
  def findCustomerById(deltaLog: DeltaLiteLog, id: Int): Option[Customer] = {
    findCustomerById(new DeltaLiteTable(deltaLog.tablePath), id)
  }
  
  /**
   * Update a customer
   */
  def updateCustomer(table: DeltaLiteTable, id: Int, newName: String, newEmail: String): Long = {
    // Find the customer first to ensure it exists
    val customer = findCustomerById(table, id)
    if (customer.isEmpty) {
      throw new IllegalArgumentException(s"Customer with ID $id not found")
    }
    
    // Update the customer
    table.update(
      converter = Customer.fromMap,
      recordToMap = (c: Customer) => c.toMap,
      condition = (c: Customer) => c.id == id,
      update = (c: Customer) => Customer(id, newName, newEmail)
    )
  }
  
  // Overload for backward compatibility with tests
  def updateCustomer(deltaLog: DeltaLiteLog, id: Int, newName: String, newEmail: String): Long = {
    updateCustomer(new DeltaLiteTable(deltaLog.tablePath), id, newName, newEmail)
  }
  
  /**
   * Delete a customer
   */
  def deleteCustomer(table: DeltaLiteTable, id: Int): Long = {
    // Find the customer first to ensure it exists
    val customer = findCustomerById(table, id)
    if (customer.isEmpty) {
      throw new IllegalArgumentException(s"Customer with ID $id not found")
    }
    
    // Delete the customer
    table.delete(
      converter = Customer.fromMap,
      recordToMap = (c: Customer) => c.toMap,
      condition = (c: Customer) => c.id == id
    )
  }
  
  // Overload for backward compatibility with tests
  def deleteCustomer(deltaLog: DeltaLiteLog, id: Int): Long = {
    deleteCustomer(new DeltaLiteTable(deltaLog.tablePath), id)
  }
  
  /**
   * Read table metadata
   */
  def readTableMetadata(table: DeltaLiteTable): Option[Metadata] = {
    DeltaLiteTable.readTableMetadata(table)
  }
  
  // Overload for backward compatibility with tests
  def readTableMetadata(deltaLog: DeltaLiteLog): Option[Metadata] = {
    readTableMetadata(new DeltaLiteTable(deltaLog.tablePath))
  }
  
  /**
   * Read commit info for a version
   */
  def readCommitInfo(deltaLog: DeltaLiteLog, version: Long): Option[CommitInfo] = {
    deltaLog.readVersion(version).toSeq.headOption.flatMap { line =>
      if (line.contains("\"commitInfo\"")) {
        val timestamp = extractJsonValue(line, "timestamp").toLong
        val operation = extractJsonValue(line, "operation")
        Some(CommitInfo(timestamp = timestamp, operation = operation))
      } else {
        None
      }
    }
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
} 