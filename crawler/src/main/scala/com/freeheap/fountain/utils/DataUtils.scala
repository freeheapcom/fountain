package com.freeheap.fountain.utils

import akka.util.ByteString
import redis.RedisClient
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import redis.{ByteStringSerializer, RedisClient, ByteStringFormatter}
import redis.RedisClient

import scala.concurrent.Await
import scala.io.Source

/**
 * Created by minhdo on 1/21/16.
 */
object DataUtils {
  val SOURCE = "urls"
  val TARGET = "urls_target"
  implicit val akkaSystem = akka.actor.ActorSystem()
  val redis = RedisClient()

  def load(filename: String) : Unit = {
    for (line <- Source.fromFile(filename).getLines()) {
      println(line)
      val r = redis.sadd(SOURCE, line)
      Await.result(r, 1 seconds)
    }
  }

  def getItem(keyspace: String) : String = {
    var result : String = null
    val r = for {
      urlOpt <- redis.spop(keyspace)
    } yield {
        urlOpt.map(url => {
          println(url)
          result = url.utf8String
        })
      }

    Await.result(r, 1 seconds)
    result
  }

  def setItem(keyspace: String, url : String) : Unit = {
    redis.sadd(keyspace, url)
  }

  def shutdown() : Unit = {
    akkaSystem.shutdown()
  }
}

