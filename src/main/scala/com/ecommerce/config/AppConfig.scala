package com.ecommerce.config

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val config = ConfigFactory.load()
  
  // Input configuration
  lazy val inputPath: String = config.getString("ecommerce.input.path")
  lazy val inputFormat: String = config.getString("ecommerce.input.format")
  
  // Output configuration  
  lazy val outputPath: String = config.getString("ecommerce.output.path")
  lazy val outputFormat: String = config.getString("ecommerce.output.format")
  
  // Spark configuration
  lazy val sparkAppName: String = config.getString("ecommerce.spark.app.name")
  lazy val sparkMaster: String = config.getString("ecommerce.spark.master")
  lazy val adaptiveQueryEnabled: Boolean = config.getBoolean("ecommerce.spark.sql.adaptive.enabled")
  
  // Business configuration
  lazy val highValueThreshold: Double = config.getDouble("ecommerce.business.high_value_threshold")
  lazy val bulkOrderQuantity: Int = config.getInt("ecommerce.business.bulk_order_quantity")
  
  // Validation (this is important for production code)
  require(inputPath.nonEmpty, "Input path cannot be empty")
  require(highValueThreshold > 0, "High value threshold must be positive")
  
  // Method to print configuration (useful for debugging)
  def printConfig(): Unit = {
    println("=== Application Configuration ===")
    println(s"Input Path: $inputPath")
    println(s"Spark App Name: $sparkAppName")
    println(s"High Value Threshold: $highValueThreshold")
  }
}