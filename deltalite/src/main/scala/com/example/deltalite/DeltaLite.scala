package com.example.deltalite

import com.example.deltalite.examples.TimeTravelExample
import java.nio.file.{Files, Paths}

object DeltaLite {
  def main(args: Array[String]): Unit = {
    println("Starting DeltaLite Time Travel Example")
    println("======================================")
    
    // Run the time travel example
    TimeTravelExample.main(args)
    
    println("======================================")
    println("DeltaLite Time Travel Example completed")
  }
}
