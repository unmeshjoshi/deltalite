package com.example.deltalite.examples

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import com.example.deltalite._
import com.example.deltalite.model.Customer
import java.io.File
import scala.util.Try

class EnhancedCustomerExampleTest extends AnyFunSuite with BeforeAndAfterEach {
  
  // Use a unique directory for each test run
  private val tempDir = System.getProperty("java.io.tmpdir")
  private var testPath: String = _
  
  override def beforeEach(): Unit = {
    // Create a new unique test directory before each test
    testPath = new File(tempDir, s"customer_test_${System.currentTimeMillis()}").getAbsolutePath
    // Ensure the directory exists and is empty
    val dir = new File(testPath)
    if (dir.exists()) {
      deleteRecursively(dir)
    }
    dir.mkdirs()
  }
  
  override def afterEach(): Unit = {
    // Clean up test directory after each test
    deleteRecursively(new File(testPath))
  }
  
  // Helper method to recursively delete a directory
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
  
  test("EnhancedCustomerExample full workflow") {
    // Create the table
    val table = DeltaLiteTable.forPath(testPath)
    
    // Define the schema
    val customerSchema = """
      {
        "type": "struct",
        "fields": [
          {"name": "id", "type": "integer", "nullable": false},
          {"name": "name", "type": "string", "nullable": false},
          {"name": "email", "type": "string", "nullable": true},
          {"name": "active", "type": "boolean", "nullable": false}
        ]
      }
    """
    
    // 1. CREATE: Test table creation
    val createVersion = table.createTable(
      name = "customers_test",
      schemaJson = customerSchema,
      configuration = Map("deltalite.version" -> "1.0")
    )
    assert(createVersion == 0, "First version should be 0")
    assert(table.version == 0, "Table version should be 0 after creation")
    assert(table.metadata.isDefined, "Table metadata should be defined")
    assert(table.metadata.get.name == "customers_test", "Table name should match")
    
    // 2. INSERT: Test inserting customers
    val customers = Seq(
      Customer(1, "John Doe", "john@example.com").toMap + ("active" -> "true"),
      Customer(2, "Jane Doe", "jane@example.com").toMap + ("active" -> "true")
    )
    
    val insertVersion = table.insert(customers)
    assert(insertVersion == 1, "Insert should create version 1")
    assert(table.version == 1, "Table version should be 1 after insert")
    
    // 3. READ: Test reading all customers
    val allCustomers = table.readAll { map =>
      (
        map("id").toInt,
        map("name"),
        map("email"),
        map("active").toBoolean
      )
    }
    assert(allCustomers.size == 2, "Should have 2 customers")
    assert(allCustomers.exists(_._1 == 1), "Should contain customer with ID 1")
    assert(allCustomers.exists(_._1 == 2), "Should contain customer with ID 2")
    
    // Type aliases for clarity
    type CustomerRecord = (Int, String, String, Boolean)
    
    // 4. UPDATE: Test updating a customer
    val updateVersion = table.update[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      recordToMap = (c: Customer) => c.toMap + ("active" -> "true"),
      condition = (c: Customer) => c.id == 2,
      update = (c: Customer) => Customer(c.id, "Jane Doe Updated", "jane.updated@example.com")
    )
    assert(updateVersion == 2, "Update should create version 2")
    
    // 5. READ: Test finding a specific customer
    val customer2 = table.find[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      predicate = (c: Customer) => c.id == 2
    )
    assert(customer2.nonEmpty, "Should find customer with ID 2")
    assert(customer2.head.name == "Jane Doe Updated", "Customer name should be updated")
    assert(customer2.head.email == "jane.updated@example.com", "Customer email should be updated")
    
    // 6. SOFT DELETE: Test soft-deleting a customer (setting active=false)
    val softDeleteVersion = table.update[CustomerRecord](
      converter = map => (map("id").toInt, map("name"), map("email"), map("active").toBoolean),
      recordToMap = { case (id: Int, name: String, email: String, active: Boolean) => 
        Map("id" -> id.toString, "name" -> name, "email" -> email, "active" -> active.toString)
      },
      condition = { case (id: Int, _, _, _) => id == 1 },
      update = { case (id: Int, name: String, email: String, _) => (id, name, email, false) }
    )
    assert(softDeleteVersion == 3, "Soft delete should create version 3")
    
    // 7. READ: Test getting only active customers
    val activeCustomers = table.find[CustomerRecord](
      converter = map => (map("id").toInt, map("name"), map("email"), map("active").toBoolean),
      predicate = { case (_, _, _, active: Boolean) => active }
    )
    assert(activeCustomers.size == 1, "Should have 1 active customer")
    assert(activeCustomers.head._1 == 2, "Active customer should have ID 2")
    
    // 8. HARD DELETE: Test hard-deleting a customer
    val hardDeleteVersion = table.delete[Int](
      converter = map => map("id").toInt,
      recordToMap = (id: Int) => Map("id" -> id.toString),
      condition = (id: Int) => id == 2
    )
    assert(hardDeleteVersion == 4, "Hard delete should create version 4")
    
    // 9. READ: Test final customer count
    val finalCustomers = table.readAll { map => map("id").toInt }
    assert(finalCustomers.size == 1, "Should have 1 customer left (the soft-deleted one)")
    
    // 10. HISTORY: Test transaction history
    val history = table.history()
    assert(history.size == 5, "Should have 5 history entries (0 through 4)")
    assert(history.map(_._1).toSet == Set(0, 1, 2, 3, 4), "Should have versions 0-4")
    
    // 11. CUSTOM TRANSACTION: Test executing a custom transaction
    val customTxnVersion = table.executeTransaction(
      isolation = SnapshotIsolation,
      operation = "TEST_OPERATION",
      transactionBody = { txn =>
        Seq(
          com.example.deltalite.actions.CommitInfo(
            operation = "TEST_OPERATION",
            operationParameters = Map("test" -> "true")
          )
        )
      }
    )
    assert(customTxnVersion == 5, "Custom transaction should create version 5")
  }
} 