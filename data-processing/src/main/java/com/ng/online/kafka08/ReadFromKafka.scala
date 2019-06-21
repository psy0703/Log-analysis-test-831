package com.ng.online.kafka08

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Spark Streaming 读取kafka 0.8 版本 Demo
 *
  * @Author: Cedaris
  * @Date: 2019/6/21 11:29
  */
object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://psy831:9000/checkpoint"
    System.setProperty("HADOOP_USER_NAME","psy831")
    //1、创建SparkConf对象
    val conf: SparkConf = new SparkConf().setAppName("ReadFromKafka").setMaster("local[*]")
    // 配置Spark Streaming查询1s从kafka一个分区消费的最大的message数
    conf.set("spark.streaming.maxRatePerPartition","100")
    // 优雅停止Spark
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //配置Kryo序列化方式
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator","com.ng.registrator.MyKryoRegistrator")

    //2、创建SparkContext对象
    val sc = new SparkContext(conf)
    //3、创建StreamingcContext
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint(checkpointDir)

    val topic = "headline"
    val groupid = "g1"

    val kafkapara: Map[String, String] = Map(
      "bootstrap.servers" -> "psy831:9092,psy832:9092,psy833:9092",
      "group.id" -> groupid
    )

    val cluster = new KafkaCluster(kafkapara)
    //获取offset
    val partitionToLong: Map[TopicAndPartition, Long] = getOffset(cluster,groupid,topic)
    //读取kafka数据
    val kafkaStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
      String](
      ssc,
      kafkapara,
      partitionToLong,
      (mess: MessageAndMetadata[String, String]) => mess.message() //数据的处理方式
    )

    kafkaStream.foreachRDD{
      x => {
        x.foreach{
          y => {
            val words: Array[String] = y.split(",",46)
            val userid = words(0)
            println("userid:" + userid)
          }
        }
      }
    }
    //提交offset
    setOffset(groupid,cluster,kafkaStream)

    /*val user_id: DStream[String] = kafkaStream.map {
      record => {
        val words: Array[String] = record.split(",", 46)
        words(0)
      }
    }*/

    ssc.start()
    ssc.awaitTermination()
    //优雅地销毁StreamingContext对象，不能销毁SparkContext对象
    ssc.stop(false,true)
  }



  def getOffset(kafkaCluster: KafkaCluster, groupid: String, topic: String): Map[TopicAndPartition, Long] ={
    //声明每一个主题分区对应的offfset
    var partitionToLong = new HashMap[TopicAndPartition, Long]
    //获取主题分区
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //判断传入的主题是否有分区信息
    if (topicAndPartitions.isRight){
      //取出分区主题
      val topicAndPartition: Set[TopicAndPartition] = topicAndPartitions.right.get
      //获取offset
      val topicAndPasrtitionToOffset: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupid, topicAndPartition)

      //判断是否消费过
      if(topicAndPasrtitionToOffset.isLeft){
        //没有消费过，赋值0,（从头消费）
        for (tp <- topicAndPartition) {
          partitionToLong +=(tp -> 0L)
        }
      }else{
        //消费过数据，取出其中的offset
        val topicOffset: Map[TopicAndPartition, Long] = topicAndPasrtitionToOffset.right.get
        partitionToLong ++= topicOffset
      }
    }
    partitionToLong
  }

  def setOffset(groupid:String,kafkaCluster: KafkaCluster,
                kafkaStream:InputDStream[String]): Unit ={
    kafkaStream.foreachRDD(rdd => {
      //获取rdd中的offset数组
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //遍历offset数组（分区）
      for (offsetRange <- offsetRanges) {
        //获取当前主题以及分区
        val topicAndPartition: TopicAndPartition = offsetRange.topicAndPartition()
        //取出offset
        val offset: Long = offsetRange.untilOffset
        //提交offset
        val ack: Either[Err, Map[TopicAndPartition, Short]] = kafkaCluster.setConsumerOffsets(groupid, Map(topicAndPartition ->
          offset))

        if(ack.isLeft){
          println(s"Error:${ack.left.get}")
        }else{
          println(s"Success:${offsetRange.partition}:$offset")
        }
      }
    })
  }

}
