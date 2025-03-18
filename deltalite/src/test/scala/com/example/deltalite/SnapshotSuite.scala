package com.example.deltalite

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files
import com.example.deltalite.actions._

class SnapshotSuite extends AnyFlatSpec with Matchers {
  
  "Snapshot" should "have correct version" in {
    val tempDir = Files.createTempDirectory("snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    val snapshot = new Snapshot(5L, deltaLog)
    
    snapshot.version should be (5L)
  }
  
  it should "have empty files list for negative version" in {
    val tempDir = Files.createTempDirectory("snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    val snapshot = new Snapshot(-1L, deltaLog)
    
    snapshot.state.files shouldBe empty
  }
  
  it should "have empty files list when no log files exist" in {
    val tempDir = Files.createTempDirectory("snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    val snapshot = new Snapshot(0L, deltaLog)
    
    snapshot.state.files shouldBe empty
  }
  
  it should "include actions from all versions up to the specified version" in {
    val tempDir = Files.createTempDirectory("snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Write actions to several versions
    deltaLog.write(1L, Seq(CommitInfo(operation = "Version 1 Action")))
    deltaLog.write(2L, Seq(CommitInfo(operation = "Version 2 Action")))
    deltaLog.write(3L, Seq(CommitInfo(operation = "Version 3 Action")))
    
    // Create snapshot at version 2
    val snapshot = new Snapshot(2L, deltaLog)
    
    // Check commit info
    snapshot.state.commitInfo should not be None
    snapshot.state.commitInfo.map(_.operation) should contain oneOf("Version 1 Action", "Version 2 Action")
    snapshot.state.commitInfo.map(_.operation) should not contain ("Version 3 Action")
  }
  
  it should "lazily compute the state" in {
    val tempDir = Files.createTempDirectory("snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    val snapshot = new Snapshot(0L, deltaLog)
    
    // Write after creating the snapshot
    deltaLog.write(1L, Seq(CommitInfo(operation = "CREATE TABLE")))
    
    // State should still have empty files list since version is 0
    snapshot.state.files shouldBe empty
  }
}

class DeltaLiteLogSnapshotSuite extends AnyFlatSpec with Matchers {
  
  "DeltaLiteLog" should "return up-to-date snapshot" in {
    val tempDir = Files.createTempDirectory("deltalog-snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Initial snapshot should have version 0 (no versions)
    deltaLog.snapshot.version should be (0L)
    
    // Write a version
    deltaLog.write(1L, Seq(CommitInfo(operation = "CREATE TABLE")))
    
    // Snapshot should be updated
    deltaLog.snapshot.version should be (1L)
    
    // Write another version
    deltaLog.write(2L, Seq(CommitInfo(operation = "INSERT")))
    
    // Snapshot should be updated again
    deltaLog.snapshot.version should be (2L)
  }
  
  it should "update snapshot when new versions are found" in {
    val tempDir = Files.createTempDirectory("deltalog-snapshot-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Initial snapshot
    val snapshot1 = deltaLog.snapshot
    snapshot1.version should be (0L)
    
    // Write a version
    deltaLog.write(1L, Seq(CommitInfo(operation = "CREATE TABLE")))
    
    // Update should return new snapshot
    val snapshot2 = deltaLog.update()
    snapshot2.version should be (1L)
    
    // Snapshot property should also be updated
    deltaLog.snapshot.version should be (1L)
  }
} 