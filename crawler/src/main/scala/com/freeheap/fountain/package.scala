package com.freeheap

import com.freeheap.fountain.rdd.SparkContextFunctions
import org.apache.spark.SparkContext

/**
 * Created by minhdo on 12/2/15.
 */

package object fountain {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

}