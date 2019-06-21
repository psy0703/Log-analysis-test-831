package com.ng.spark

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

/**
  * Spark读取kafka 011 版本
  * @Author: Cedaris
  * @Date: 2019/6/21 14:15
  */
object readKafka {
  def main(args: Array[String]): Unit = {
    val ckPath = "/checkpoint"
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("readKafka")
      .set("spark.streaming.maxRatePerPartition", "100")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //配置Kryo序列化方式
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Milliseconds(1000))
    ssc.checkpoint(ckPath)

    val preferredHosts: LocationStrategy = LocationStrategies.PreferBrokers
    val bootstrap = "psy831:9092,psy832:9092,psy833:9092"
    val topics = Array("headline")
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test02",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    //将读取的数据处理并写入Redis
    kafkaStream.foreachRDD { rdd =>
      rdd.repartition(300).foreachPartition(eachpartition => {
        //获取redis连接
        val redisHost = "127.0.0.1"
        val redisPort = 6379
        val jedis = new Jedis(redisHost, redisPort)

        eachpartition.foreach { x => {
          val words: Array[String] = x.value().split(",", 46)
          val user_id: String = words(0)
          val expireDays = 1
          val key = getCurrentHour
          println("word -->>" + key + " : " + user_id)
          jedis.pfadd(key, user_id) //求每小时流量
          // 设置存储每天的数据的set过期时间，防止超过redis容量，这样每天的set集合，定期会被自动删除
          jedis.expire(key, expireDays * 3600 * 24)
        }
          jedis.close()
        }
      })
    }

    ssc.start()
    ssc.awaitTermination()
    //优雅地销毁StreamingContext对象，不能销毁SparkContext对象
    ssc.stop(false, true)
  }

  def getCurrentHour: String = {
    val time: Long = System.currentTimeMillis()
    val date = new Date(time)
    val sdf = new SimpleDateFormat("yyyyMMdd-HH")
    val currentHour: String = sdf.format(date)
    currentHour
  }
}
