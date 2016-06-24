package com.freeheap.fountain.rdd


/**
 * Created by minhdo on 12/2/15.
 */
import java.net.InetAddress

import org.apache.spark.Partition



/**
  * @param index identifier of the partition, used internally by Spark
  */
case class LinkPartition(index: Int) extends Partition