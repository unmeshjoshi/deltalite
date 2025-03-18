package com.example.deltalite.model

/**
 * Simple Customer model for our examples
 */
case class Customer(id: Int, name: String, email: String) {
  /**
   * Convert to a map representation for storage
   */
  def toMap: Map[String, String] = Map(
    "id" -> id.toString,
    "name" -> name,
    "email" -> email
  )
}

object Customer {
  /**
   * Create a Customer from a map representation
   */
  def fromMap(map: Map[String, String]): Customer = {
    Customer(
      id = map("id").toInt,
      name = map("name"),
      email = map("email")
    )
  }
} 