package com.freeheap.fountain.rdd

/**
 * Created by minhdo on 12/2/15.
 */

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.rdd.EmptyCassandraRDD
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.rdd.{CassandraTableScanRDD, EmptyCassandraRDD, ReadConf, ValidRDDType}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on [[org.apache.spark.SparkContext SparkContext]] */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {



  def linksData(appName: String)
                      (implicit
                       ev: ValidRDDType[String]) =
    new LinkRDD(sc, Seq.empty)
}
