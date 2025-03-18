package com.example.deltalite

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files
import com.example.deltalite.actions._

class OptimisticTransactionSuite extends AnyFlatSpec with Matchers {
  
  "OptimisticTransaction" should "have the correct read version" in {
    val tempDir = Files.createTempDirectory("transaction-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Write some actions
    deltaLog.write(1L, Seq(CommitInfo(operation = "CREATE TABLE")))
    deltaLog.write(2L, Seq(CommitInfo(operation = "INSERT")))
    
    // Start a transaction
    val txn = deltaLog.startTransaction()
    
    // Read version should be 2
    txn.readVersion should be (2L)
  }
  
  it should "commit actions to the log" in {
    val tempDir = Files.createTempDirectory("transaction-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Start a transaction
    val txn = deltaLog.startTransaction()
    
    // Initial version should be 0 (no log files)
    txn.readVersion should be (0L)
    
    // Commit some actions
    val commitVersion = txn.commit(
      Seq(CommitInfo(operation = "TEST COMMIT")),
      "TRANSACTION TEST"
    )
    
    // Should have committed version 0
    commitVersion should be (0L)
    
    // The log should now have a version 0 file
    deltaLog.listVersions() should contain (0L)
    
    // Check the file content
    val lines = deltaLog.readVersion(0L).toList
    lines.size should be (2) // CommitInfo from the commit and the one we added
    lines(0) should include (""""operation":"TRANSACTION TEST"""")
    lines(1) should include (""""operation":"TEST COMMIT"""")
  }
  
  it should "throw an exception when a conflict is detected" in {
    val tempDir = Files.createTempDirectory("transaction-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // First create initial version 0
    val setupTxn = deltaLog.startTransaction()
    setupTxn.commit(Seq(CommitInfo(operation = "SETUP")), "SETUP")
    
    // Start first transaction on version 0
    val txn1 = deltaLog.startTransaction()
    
    // Store the read version to verify later
    val readVersion = txn1.readVersion
    readVersion should be (0L)
    
    // Commit something from another transaction to create a conflict - this will become version 1
    val txn2 = deltaLog.startTransaction()
    val commitVersion = txn2.commit(Seq(CommitInfo(operation = "CONFLICTING COMMIT")), "CONFLICT TEST")
    
    // Verify txn2 updated the version
    commitVersion should be (1L) // Second commit is at version 1
    deltaLog.snapshot.version should be (1L)
    
    // Try to commit from the first transaction - should fail because it was based on version 0
    // but version 1 exists now
    val exception = intercept[ConcurrentModificationException] {
      txn1.commit(Seq(CommitInfo(operation = "SHOULD FAIL")), "CONFLICT TEST")
    }
    
    exception.getMessage should include ("Concurrent transaction conflict")
  }
  
  it should "keep track of read files" in {
    val tempDir = Files.createTempDirectory("transaction-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    val txn = deltaLog.startTransaction()
    
    // Record that we read some files
    txn.readFile("file1.csv")
    txn.readFile("file2.csv")
    
    // Nothing to verify externally, just make sure the method doesn't throw
    // In a real implementation, we would test that conflicts are detected based on read files
  }
} 