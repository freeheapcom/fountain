package com.freeheap.fountain.sparkjobs

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Minh Do
  * Created on 6/6/16.
  */
object CassandraSparkJob {

    //need to start a local cassandra first or
    // deploy this whole jar under a cloud to run it from there with a valid contact point
    def main(args: Array[String]) = {
        if (args.length != 3) {
            System.err.println("Usage: CassandraSparkJob <cassandraContactPoint> <keyspace> <nativePort>")
            System.exit(1)
        }
        val Array(cassandraContactPoint, keyspace, nativePort) = args

        println(cassandraContactPoint)
        println(keyspace)
        println(nativePort)

        val conf = new SparkConf()
            .setAppName("CassandraSparkJob")
            .set("spark.cassandra.connection.host", cassandraContactPoint)
            .set("spark.cassandra.connection.native.port", nativePort)
            .set("spark.cassandra.output.consistency.level", "CL_LOCAL_ONE")
            .setMaster("local[2]")

        val sc = new SparkContext(conf)
        val connector = CassandraConnector(sc.getConf)
        new DataInit(connector, keyspace).init()

        val rdd = sc.cassandraTable(keyspace, "users").where("first_name < ?", "first_6")

        println(rdd.count)
        rdd.foreach(println)

        sc.stop()
    }

}
