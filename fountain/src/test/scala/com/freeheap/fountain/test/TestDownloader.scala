package com.fountain.test

import org.apache.spark.{Logging, SparkContext, SparkConf}

trait DemoApp extends App with Logging {

  val words = "./spark-cassandra-connector-demos/simple-demos/src/main/resources/data/words"

  val SparkMasterHost = "127.0.0.1" //54.221.134.46"

  val CassandraHost = "127.0.0.1"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .set("spark.cassandra.connection.port", "9042")
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[12]")
    .setAppName(getClass.getSimpleName)

  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)

  println("Done!")
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}
}
