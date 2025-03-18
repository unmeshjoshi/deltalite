package com.example.deltalite.examples

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files
import com.example.deltalite._
import com.example.deltalite.actions._
import com.example.deltalite.model.Customer

class CustomerCrudExampleSuite extends AnyFlatSpec with Matchers {

  "CustomerCrudExample" should "create a table with metadata" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table
    val version = CustomerCrudExample.createCustomerTable(deltaLog)
    println(s"Table created with version: $version")
    version should be (0L)
    
    // Verify metadata exists
    val logPath = s"$tablePath/_delta_log"
    val logDir = new File(logPath)
    println(s"Log directory exists: ${logDir.exists()}")
    if (logDir.exists()) {
      val logFiles = logDir.listFiles()
      println(s"Log files: ${logFiles.mkString(", ")}")
      for (file <- logFiles) {
        if (file.getName.endsWith(".json")) {
          println(s"Contents of ${file.getName}:")
          val content = scala.io.Source.fromFile(file).getLines().mkString("\n")
          println(content)
        }
      }
    }
    
    // Verify metadata
    val metadata = CustomerCrudExample.readTableMetadata(deltaLog)
    println(s"Metadata: $metadata")
    metadata.isDefined should be (true)
    
    // Check that the name and description fields are non-null
    if (metadata.isDefined) {
      println(s"Metadata name: ${metadata.get.name}")
      println(s"Metadata description: ${metadata.get.description}")
      println(s"Metadata schema: ${metadata.get.schemaString}")
    }
    
    metadata.get.name should not be null
    metadata.get.description should not be null
    
    // Check schema contains required fields
    val schema = metadata.get.schemaString
    schema should not be null
    
    // Only check inclusion if schema is not null
    if (schema != null) {
      schema should include ("integer")
      schema should include ("string")
    }
    
    metadata.get.configuration should contain key ("deltalite.version")
  }
  
  it should "insert customers correctly" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table
    CustomerCrudExample.createCustomerTable(deltaLog)
    
    // Insert some customers
    val customers = Seq(
      Customer(1, "Test User 1", "test1@example.com"),
      Customer(2, "Test User 2", "test2@example.com")
    )
    
    val version = CustomerCrudExample.insertCustomers(deltaLog, customers)
    version should be (1L)
    
    // Read back the customers
    val readCustomers = CustomerCrudExample.readAllCustomers(deltaLog)
    readCustomers.size should be (2)
    readCustomers.map(_.id).toSet should be (Set(1, 2))
    readCustomers.find(_.id == 1).get.name should be ("Test User 1")
    readCustomers.find(_.id == 2).get.name should be ("Test User 2")
  }
  
  it should "update a customer correctly" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table and insert customers
    CustomerCrudExample.createCustomerTable(deltaLog)
    CustomerCrudExample.insertCustomers(deltaLog, Seq(
      Customer(1, "Test User 1", "test1@example.com"),
      Customer(2, "Test User 2", "test2@example.com")
    ))
    
    // Update customer 2
    val version = CustomerCrudExample.updateCustomer(
      deltaLog, 2, "Updated Name", "updated@example.com")
    version should be (2L)
    
    // Verify the update
    val customer = CustomerCrudExample.findCustomerById(deltaLog, 2)
    customer.isDefined should be (true)
    customer.get.name should be ("Updated Name")
    customer.get.email should be ("updated@example.com")
    
    // Verify customer 1 wasn't modified
    val customer1 = CustomerCrudExample.findCustomerById(deltaLog, 1)
    customer1.isDefined should be (true)
    customer1.get.name should be ("Test User 1")
  }
  
  it should "delete a customer correctly" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table and insert customers
    CustomerCrudExample.createCustomerTable(deltaLog)
    CustomerCrudExample.insertCustomers(deltaLog, Seq(
      Customer(1, "Test User 1", "test1@example.com"),
      Customer(2, "Test User 2", "test2@example.com"),
      Customer(3, "Test User 3", "test3@example.com")
    ))
    
    // Delete customer 2
    val version = CustomerCrudExample.deleteCustomer(deltaLog, 2)
    version should be (2L)
    
    // Verify the deletion
    val customers = CustomerCrudExample.readAllCustomers(deltaLog)
    customers.size should be (2)
    customers.map(_.id).toSet should be (Set(1, 3))
    
    // Verify customer 2 doesn't exist
    val customer2 = CustomerCrudExample.findCustomerById(deltaLog, 2)
    customer2.isDefined should be (false)
  }
  
  it should "throw exception when updating non-existent customer" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table and insert customers
    CustomerCrudExample.createCustomerTable(deltaLog)
    CustomerCrudExample.insertCustomers(deltaLog, Seq(
      Customer(1, "Test User 1", "test1@example.com")
    ))
    
    // Try to update non-existent customer
    val exception = intercept[IllegalArgumentException] {
      CustomerCrudExample.updateCustomer(deltaLog, 999, "Updated Name", "updated@example.com")
    }
    
    exception.getMessage should include ("not found")
  }
  
  it should "throw exception when deleting non-existent customer" in {
    val tempDir = Files.createTempDirectory("customer-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create the table and insert customers
    CustomerCrudExample.createCustomerTable(deltaLog)
    CustomerCrudExample.insertCustomers(deltaLog, Seq(
      Customer(1, "Test User 1", "test1@example.com")
    ))
    
    // Try to delete non-existent customer
    val exception = intercept[IllegalArgumentException] {
      CustomerCrudExample.deleteCustomer(deltaLog, 999)
    }
    
    exception.getMessage should include ("not found")
  }
} 