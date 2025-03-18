package com.example.deltalite.examples

import com.example.deltalite._
import com.example.deltalite.model.Customer
import java.io.File

/**
 * Example showing improved table operations with DeltaLiteTable.
 */
object EnhancedCustomerExample {
  
  def main(args: Array[String]): Unit = {
    // Create a table in a temporary directory
    val tempDir = System.getProperty("java.io.tmpdir")
    val tablePath = new File(tempDir, s"customer_table_${System.currentTimeMillis()}").getAbsolutePath
    
    println(s"Creating customer table at: $tablePath")
    
    // Create the table object
    val table = DeltaLiteTable.forPath(tablePath)
    
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
    
    // CREATE: Create the table with the schema
    val createVersion = table.createTable(
      name = "customers",
      schemaJson = customerSchema,
      configuration = Map("deltalite.version" -> "1.0")
    )
    println(s"Created table at version $createVersion")
    
    // INSERT: Insert some customers
    val customers = Seq(
      Customer(1, "John Doe", "john@example.com").toMap + ("active" -> "true"),
      Customer(2, "Jane Doe", "jane@example.com").toMap + ("active" -> "true"),
      Customer(3, "Bob Smith", "bob@example.com").toMap + ("active" -> "true"),
      Customer(4, "Alice Johnson", "alice@example.com").toMap + ("active" -> "true")
    )
    
    val insertVersion = table.insert(customers)
    println(s"Inserted customers at version $insertVersion")
    
    // READ: Read all customers
    val allCustomers = table.readAll { map =>
      (
        map("id").toInt,
        map("name"),
        map("email"),
        map("active").toBoolean
      )
    }
    println("All customers:")
    allCustomers.foreach { case (id, name, email, active) =>
      println(s"  $id: $name, $email, active=$active")
    }
    
    // UPDATE: Update a customer
    val updateVersion = table.update[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      recordToMap = (c: Customer) => c.toMap + ("active" -> "true"),
      condition = (c: Customer) => c.id == 2,
      update = (c: Customer) => Customer(c.id, "Jane Doe Updated", "jane.updated@example.com")
    )
    println(s"Updated customer at version $updateVersion")
    
    // READ: Find a specific customer
    val customer2 = table.find[Customer](
      converter = map => Customer(map("id").toInt, map("name"), map("email")),
      predicate = (c: Customer) => c.id == 2
    )
    println(s"Customer 2: ${customer2.headOption.map(c => s"${c.name} (${c.email})").getOrElse("not found")}")
    
    // Type aliases for clarity
    type CustomerRecord = (Int, String, String, Boolean)
    
    // DELETE: Delete a customer by setting active=false (soft delete)
    val softDeleteVersion = table.update[CustomerRecord](
      converter = map => (map("id").toInt, map("name"), map("email"), map("active").toBoolean),
      recordToMap = { case (id: Int, name: String, email: String, active: Boolean) => 
        Map("id" -> id.toString, "name" -> name, "email" -> email, "active" -> active.toString)
      },
      condition = { case (id: Int, _, _, _) => id == 3 },
      update = { case (id: Int, name: String, email: String, _) => (id, name, email, false) }
    )
    println(s"Soft-deleted customer (set active=false) at version $softDeleteVersion")
    
    // READ: Get only active customers
    val activeCustomers = table.find[CustomerRecord](
      converter = map => (map("id").toInt, map("name"), map("email"), map("active").toBoolean),
      predicate = { case (_, _, _, active: Boolean) => active }
    )
    println(s"Active customers (${activeCustomers.size}):")
    activeCustomers.foreach { case (id, name, email, _) =>
      println(s"  $id: $name, $email")
    }
    
    // DELETE: Hard delete a customer
    val hardDeleteVersion = table.delete[Int](
      converter = map => map("id").toInt,
      recordToMap = (id: Int) => Map("id" -> id.toString),
      condition = (id: Int) => id == 4
    )
    println(s"Hard-deleted customer at version $hardDeleteVersion")
    
    // READ: Get all customers after the hard delete
    val finalCustomers = table.readAll { map =>
      (
        map("id").toInt,
        map("name"),
        map("email"),
        map("active").toBoolean
      )
    }
    println(s"Final customer list (${finalCustomers.size}):")
    finalCustomers.foreach { case (id, name, email, active) =>
      println(s"  $id: $name, $email, active=$active")
    }
    
    // View transaction history
    val history = table.history()
    println("Transaction history:")
    history.foreach { case (version, operation, timestamp) =>
      println(s"  Version $version: $operation at ${new java.util.Date(timestamp)}")
    }
    
    // Execute a custom transaction
    val customTxnVersion = table.executeTransaction(
      isolation = SnapshotIsolation,
      operation = "CUSTOM_OPERATION",
      transactionBody = { txn =>
        // Read a file but don't modify it
        val files = txn.snapshot.allFiles
        if (files.nonEmpty) {
          txn.readFile(files.head.path)
        }
        
        // Add a commit info action only
        Seq(
          com.example.deltalite.actions.CommitInfo(
            operation = "CUSTOM_OPERATION",
            operationParameters = Map("note" -> "This is a custom transaction")
          )
        )
      }
    )
    println(s"Custom transaction completed at version $customTxnVersion")
    
    println(s"Table operations complete. Final version: ${table.version}")
  }
} 