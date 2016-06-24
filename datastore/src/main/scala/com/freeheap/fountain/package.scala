package com.freeheap


import org.apache.spark.SparkContext


/**
  * @author Minh Do
  * Created on 6/6/16.
  */

package object fountain {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

}
