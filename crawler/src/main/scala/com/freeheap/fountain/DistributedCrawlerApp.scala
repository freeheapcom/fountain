package com.freeheap.fountain

import com.freeheap.fountain.utils.DataUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by minhdo on 3/12/16.
 */
object DistributedCrawlerApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Distributed Crawler App").setMaster("local[2]")
    val sc = new SparkContext(conf)

    DataUtils.load("data/urls.txt")

    val rdd = sc.linksData("Distributed Crawler").cache()
    rdd.foreach(row => println(s"New Data: $row.url"))

    println("==============================================")
    println("count = " + rdd.count())

  }
}
