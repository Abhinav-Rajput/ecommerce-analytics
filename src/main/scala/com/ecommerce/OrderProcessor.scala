package com.ecommerce

import org.apache.spark.sql.SparkSession

object OrderProcessor {
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark Session (understand this deeply)
    val spark = SparkSession.builder()
      .appName("EcommerceAnalytics")
      .master("local[2]")  // Start with just 2 cores
      .getOrCreate()
    
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
    
    spark.stop()
  }
}