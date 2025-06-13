package com.ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OrderProcessor {
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark Session (understand this deeply)
    val spark = SparkSession.builder()
      .appName("EcommerceAnalytics")
      .master("local[2]")  // Start with just 2 cores
      .getOrCreate()
    
    // Import implicits for $ notation
    import spark.implicits._
    
    println("Spark Session Created Successfully!")
    println(s"Spark Version: ${spark.version}")
    println(s"Spark App Name: ${spark.sparkContext.appName}")
    
    // Step 2: Read a simple CSV (understand DataFrame creation)
    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/orders.csv")
    
    // Step 3: Basic operations (understand lazy evaluation)
    println("=== Schema ===")
    ordersDF.printSchema()
    
    println("=== Sample Data ===")
    ordersDF.show(5)
    
    println("=== Row Count ===")
    println(s"Total Orders: ${ordersDF.count()}")
    
    // DAY 2 NEW CONTENT: Basic transformations - understand each one
    println("\n=== DAY 2: TRANSFORMATIONS & AGGREGATIONS ===")
    
    val transformedDF = ordersDF
      .withColumn("total_amount", $"quantity" * $"price")
      .withColumn("order_month", month($"order_date"))
      .filter($"price" > 50)  // Understand predicate pushdown

    println("=== Transformed Data ===")
    transformedDF.show()
    
    // Basic aggregations - understand groupBy mechanics
    val customerSummary = transformedDF
      .groupBy("customer_id")
      .agg(
        count("order_id").as("total_orders"),
        sum("total_amount").as("total_spent"),
        avg("total_amount").as("avg_order_value")
      )

    println("=== Customer Summary ===")
    customerSummary.show()
    
    // EXPLORATION EXERCISES FOR DAY 2:
    println("\n=== EXPLORATION: Different Aggregations ===")
    
    // Product-level analysis
    val productSummary = transformedDF
      .groupBy("product_name")
      .agg(
        count("order_id").as("times_ordered"),
        sum("quantity").as("total_quantity_sold"),
        sum("total_amount").as("total_revenue"),
        avg("price").as("avg_price")
      )
      .orderBy(desc("total_revenue"))
    
    println("=== Product Analysis ===")
    productSummary.show()
    
    // Monthly trends
    val monthlyTrends = transformedDF
      .groupBy("order_month")
      .agg(
        count("order_id").as("orders_count"),
        sum("total_amount").as("monthly_revenue"),
        countDistinct("customer_id").as("unique_customers")
      )
      .orderBy("order_month")
    
    println("=== Monthly Trends ===")
    monthlyTrends.show()
    
    // DEEP DIVE: Understand query execution
    println("\n=== QUERY EXECUTION ANALYSIS ===")
    println("Customer Summary Execution Plan:")
    customerSummary.explain()
    
    spark.stop()
  }
}