package com.freeheap.fountain.rdd

import org.apache.spark.Partition


class LinkPartitioner() {


  def partitions(): Array[Partition] = {

    (for (index <- 1 to 10) yield
      {
        LinkPartition(index)
      }).toArray
  }

}

object LinkPartitioner {

  type V = t forSome { type t }

  def apply(): LinkPartitioner = {

    new LinkPartitioner()
  }
}
