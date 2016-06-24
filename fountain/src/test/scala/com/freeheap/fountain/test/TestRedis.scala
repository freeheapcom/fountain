package com.freeheap.fountain.test

/**
 * Created by minhdo on 1/20/16.
 */
import akka.util.ByteString
import redis.RedisClient
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import redis.{ByteStringSerializer, RedisClient, ByteStringFormatter}

case class DumbClass(s1: String, s2: String)

object DumbClass {
  implicit val byteStringFormatter = new ByteStringFormatter[DumbClass] {
    def serialize(data: DumbClass): ByteString = {
      ByteString(data.s1 + "|" + data.s2)
    }

    def deserialize(bs: ByteString): DumbClass = {
      val r = bs.utf8String.split('|').toList
      DumbClass(r(0), r(1))
    }
  }
}

object TestRedis extends App {
  implicit val akkaSystem = akka.actor.ActorSystem()

  val redis = RedisClient()

  val futurePong = redis.ping()
  println("Ping sent!")
  futurePong.map(pong => {
    println(s"Redis replied with a $pong")
  })
  Await.result(futurePong, 5 seconds)


  val dumb = DumbClass("s111", "s2222")

  val r = for {
    set <- redis.set("dumbKey", dumb)
    getDumbOpt <- redis.get[DumbClass]("dumbKey")
  } yield {
      getDumbOpt.map(getDumb => {
        assert(getDumb == dumb)
        println(getDumb.s1)
      })
    }

  Await.result(r, 5 seconds)

  redis.set("dumbKey2", "abc")

  akkaSystem.shutdown()
}