package com.example.deltalite

import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

object DeltaLite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaLite")
      .master("local[*]")
      .getOrCreate()
    
    println("Initialized DeltaLite with Spark " + spark.version)
    
    spark.stop()
  }
}
