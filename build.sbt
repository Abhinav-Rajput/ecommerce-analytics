// build.sbt - Streamlined configuration for Scala + Spark interview preparation
// This version focuses on essential dependencies and clear, working setup

name := "ecommerce-analytics"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"
organization := "com.ecommerce"  // Add this line

// Essential dependencies for interview success
libraryDependencies ++= Seq(
  // Core Spark dependencies - these two cover 95% of interview scenarios
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "com.typesafe" % "config" % "1.4.2",
  // Testing framework for practicing coding challenges
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Java compatibility settings for your Java 17 environment
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
)

// Fork JVM to ensure Spark runs in clean environment
fork := true

// Compiler options that help you learn and catch errors early
scalacOptions ++= Seq(
  "-feature",           // Warn about features needing explicit import
  "-unchecked",         // Detailed warnings about type erasure
  "-deprecation",       // Warn about deprecated API usage
  "-encoding", "utf8"   // Ensure consistent character encoding
)