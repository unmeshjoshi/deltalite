package com.example.deltalite

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files
import com.example.deltalite.actions._

class DeltaLiteLogSuite extends AnyFlatSpec with Matchers {
  
  "DeltaLiteLog" should "create log directory with correct path" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    val logDir = new File(deltaLog.logPath)
    
    logDir.getAbsolutePath should be (s"$tablePath/_delta_log")
  }
  
  it should "list versions correctly when log directory is empty" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    deltaLog.listVersions() should be (Seq.empty)
  }
  
  it should "list versions correctly when log directory has version files" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    // Create log directory and some version files
    val logDir = new File(tablePath, "_delta_log")
    logDir.mkdirs()
    
    // Create some version files
    new File(logDir, "00000000000000000001.json").createNewFile()
    new File(logDir, "00000000000000000002.json").createNewFile()
    new File(logDir, "00000000000000000003.json").createNewFile()
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    deltaLog.listVersions() should be (Seq(1L, 2L, 3L))
  }
  
  it should "ignore non-version files in log directory" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    // Create log directory and some files
    val logDir = new File(tablePath, "_delta_log")
    logDir.mkdirs()
    
    // Create version files and other files
    new File(logDir, "00000000000000000001.json").createNewFile()
    new File(logDir, "00000000000000000002.json").createNewFile()
    new File(logDir, "some-other-file.txt").createNewFile()
    new File(logDir, "not-a-version.json.txt").createNewFile()
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    deltaLog.listVersions() should be (Seq(1L, 2L))
  }
  
  it should "write actions to a version file" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create a commit info action
    val commitInfo = CommitInfo(
      timestamp = 1234567890L,
      operation = "CREATE TABLE",
      operationParameters = Map("table" -> "test"),
      readVersion = Some(0L),
      isBlindAppend = Some(true)
    )
    
    // Write the action
    deltaLog.write(1L, Seq(commitInfo))
    
    // Verify the file was created
    val logDir = new File(deltaLog.logPath)
    val versionFile = new File(logDir, "00000000000000000001.json")
    versionFile.exists() should be (true)
    
    // Read and verify the content
    val content = scala.io.Source.fromFile(versionFile).getLines().mkString("\n")
    content should include (""""timestamp":1234567890""")
    content should include (""""operation":"CREATE TABLE"""")
    content should include (""""table":"test"""")
    content should include (""""readVersion":0""")
    content should include (""""isBlindAppend":true""")
  }
  
  it should "write multiple actions to a version file" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create multiple commit info actions
    val actions = Seq(
      CommitInfo(operation = "CREATE TABLE"),
      CommitInfo(operation = "INSERT"),
      CommitInfo(operation = "UPDATE")
    )
    
    // Write the actions
    deltaLog.write(1L, actions)
    
    // Verify the file was created
    val logDir = new File(deltaLog.logPath)
    val versionFile = new File(logDir, "00000000000000000001.json")
    versionFile.exists() should be (true)
    
    // Read and verify the content
    val content = scala.io.Source.fromFile(versionFile).getLines().toList
    content.size should be (3)
    content(0) should include (""""operation":"CREATE TABLE"""")
    content(1) should include (""""operation":"INSERT"""")
    content(2) should include (""""operation":"UPDATE"""")
  }
  
  it should "create log directory if it doesn't exist when writing" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Write an action without creating the directory first
    deltaLog.write(1L, Seq(CommitInfo(operation = "CREATE TABLE")))
    
    // Verify the directory and file were created
    val logDir = new File(deltaLog.logPath)
    logDir.exists() should be (true)
    logDir.isDirectory should be (true)
    
    val versionFile = new File(logDir, "00000000000000000001.json")
    versionFile.exists() should be (true)
  }
  
  it should "read actions from a version file" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create actions
    val actions = Seq(
      CommitInfo(operation = "CREATE TABLE"),
      CommitInfo(operation = "INSERT")
    )
    
    // Write the actions
    deltaLog.write(1L, actions)
    
    // Read the version file
    val lines = deltaLog.readVersion(1L).toList
    
    // Verify content
    lines.size should be (2)
    lines(0) should include (""""operation":"CREATE TABLE"""")
    lines(1) should include (""""operation":"INSERT"""")
  }
  
  it should "return empty iterator when reading non-existent version file" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Read non-existent version
    val lines = deltaLog.readVersion(999L).toList
    
    // Verify empty result
    lines.isEmpty should be (true)
  }
  
  it should "write and read actions round-trip" in {
    val tempDir = Files.createTempDirectory("deltalite-test").toFile
    val tablePath = new File(tempDir, "test-table").getAbsolutePath
    
    val deltaLog = DeltaLiteLog.forTable(tablePath)
    
    // Create complex commit info
    val commitInfo = CommitInfo(
      timestamp = 1234567890L,
      operation = "COMPLEX OPERATION",
      operationParameters = Map("param1" -> "value1", "param2" -> "value2"),
      readVersion = Some(5L),
      isBlindAppend = Some(false)
    )
    
    // Write the action
    deltaLog.write(10L, Seq(commitInfo))
    
    // Read it back
    val lines = deltaLog.readVersion(10L).toList
    
    // Verify complete round-trip
    lines.size should be (1)
    val line = lines.head
    line should include (""""timestamp":1234567890""")
    line should include (""""operation":"COMPLEX OPERATION"""")
    line should include (""""param1":"value1"""")
    line should include (""""param2":"value2"""")
    line should include (""""readVersion":5""")
    line should include (""""isBlindAppend":false""")
  }
} 