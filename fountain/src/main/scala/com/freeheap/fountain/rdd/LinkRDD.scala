package com.freeheap.fountain.rdd


import com.freeheap.fountain.utils.LinkIterator
import edu.uci.ics.crawler4j.crawler.Page
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, Dependency, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by minhdo on 12/1/15.
 */

class LinkRDD(@transient val sc: SparkContext,
                      dep: Seq[Dependency[_]])
  extends RDD[Page](sc, Seq.empty) {

  type Self <: LinkRDD



  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Page] = {

    logInfo("In compute .......")
    //println(split)
    val result = scala.collection.mutable.ListBuffer.empty[String]
    val partition = split.asInstanceOf[LinkPartition]

    //val wc = new WebpageDownloader();
    //val argv = Array(partition.endpoint, "5")
    //wc.initialize(argv);

    //import scala.collection.JavaConversions._
    //return wc
    //result.iterator

    //List(pageFetcher.).iterator

    new LinkIterator()
  }




  override protected def getPartitions: Array[Partition] = {
    //verify() // let's fail fast
    val partitioner = LinkPartitioner()
    val partitions = partitioner.partitions
    //logDebug(s"Created total ${partitions.length} partitions for $.clusterName")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }
}



object LinkRDD {
  def apply[T](sc: SparkContext)
              (implicit ct: ClassTag[T]): LinkRDD =

    new LinkRDD(sc, Nil)

}
