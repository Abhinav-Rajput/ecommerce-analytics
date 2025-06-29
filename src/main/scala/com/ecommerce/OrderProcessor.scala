package com.ecommerce

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.config.AppConfig

object OrderProcessor {
  def main(args: Array[String]): Unit = {
    // Print configuration for debugging
    AppConfig.printConfig()
    
    // Create Spark Session with configuration
    val spark = createSparkSession()
    import spark.implicits._
    
    try {
      println("Starting Order Processing...")
      
      // Step 1: Load data with error handling
      val ordersDF = loadOrderData(spark)
      
      // Step 2: Validate data quality
      validateDataQuality(ordersDF)
      
      // Step 3: Process data
      processOrders(spark, ordersDF)
      
      println("Order processing completed successfully!")
      
    } catch {
      case ex: java.io.FileNotFoundException =>
        println(s"ERROR: Input file not found: ${ex.getMessage}")
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s"ERROR: Data analysis error: ${ex.getMessage}")
      case ex: IllegalArgumentException =>
        println(s"ERROR: Invalid configuration: ${ex.getMessage}")
      case ex: Exception =>
        println(s"ERROR: Unexpected error: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      // Always clean up resources
      println("Cleaning up resources...")
      spark.stop()
    }
  }
  
  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(AppConfig.sparkAppName)
      .master(AppConfig.sparkMaster)
      .config("spark.sql.adaptive.enabled", AppConfig.adaptiveQueryEnabled)
      .getOrCreate()
  }
  
  private def loadOrderData(spark: SparkSession): DataFrame = {
    println(s"Loading data from: ${AppConfig.inputPath}")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.inputPath)
    
    // Validate that we actually loaded data
    if (df.isEmpty) {
      throw new RuntimeException("No data found in input file")
    }
    
    println(s"Successfully loaded ${df.count()} records")
    df
  }
  
  private def validateDataQuality(df: DataFrame): Unit = {
    println("=== Data Quality Validation ===")
    
    // Check for required columns
    val requiredColumns = Set("order_id", "customer_id", "quantity", "price")
    val actualColumns = df.columns.toSet
    val missingColumns = requiredColumns -- actualColumns
    
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required columns: ${missingColumns.mkString(", ")}")
    }
    
    // Check for data quality issues
    val totalRecords = df.count()
    val nullOrderIds = df.filter(col("order_id").isNull).count()
    val negativeQuantities = df.filter(col("quantity") < 0).count()
    val zeroPrices = df.filter(col("price") <= 0).count()
    
    println(s"Total Records: $totalRecords")
    println(s"Null Order IDs: $nullOrderIds")
    println(s"Negative Quantities: $negativeQuantities")
    println(s"Zero/Negative Prices: $zeroPrices")
    
    // Warn about data quality issues
    if (nullOrderIds > 0) println(s"WARNING: Found $nullOrderIds records with null order IDs")
    if (negativeQuantities > 0) println(s"WARNING: Found $negativeQuantities records with negative quantities")
    if (zeroPrices > 0) println(s"WARNING: Found $zeroPrices records with zero/negative prices")
  }
  
  private def processOrders(spark: SparkSession, ordersDF: DataFrame): Unit = {
    import spark.implicits._
    
    // Clean the data
    val cleanOrdersDF = ordersDF
      .filter(col("order_id").isNotNull)
      .filter(col("quantity") > 0)
      .filter(col("price") > 0)
    
    // Your existing transformations with business rules from config
    val transformedDF = cleanOrdersDF
      .withColumn("total_amount", $"quantity" * $"price")
      .withColumn("order_month", month($"order_date"))
      .withColumn("is_high_value", $"total_amount" >= AppConfig.highValueThreshold)
      .withColumn("is_bulk_order", $"quantity" >= AppConfig.bulkOrderQuantity)
    
    println("=== Processed Data Sample ===")
    transformedDF.show(10)
    
    // Business analysis
    val businessSummary = transformedDF
      .agg(
        count("*").as("total_orders"),
        sum("total_amount").as("total_revenue"),
        avg("total_amount").as("avg_order_value"),
        sum(when($"is_high_value", 1).otherwise(0)).as("high_value_orders"),
        sum(when($"is_bulk_order", 1).otherwise(0)).as("bulk_orders")
      )
    
    println("=== Business Summary ===")
    businessSummary.show()
  }
}