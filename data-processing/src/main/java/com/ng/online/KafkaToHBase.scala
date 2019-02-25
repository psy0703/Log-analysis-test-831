package com.ng.online

import java.lang
import java.util.{Date, Properties}

import com.ng.model.StartupReportLogs
import com.ng.utils.{DateUtils, HBaseUtils, JsonUtils}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object KafkaToHBase {

  def main(args: Array[String]): Unit = {
    //设置CheckPoint目录
    val ckPath = "/ck/kafkaToHBase/kafka"
    //恢复或重新创建ssc
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(ckPath,() => createFunc())
    //启动ssc
    ssc.start()

    ssc.awaitTermination()
  }

  def createFunc(): StreamingContext = {
    val properties = new Properties()
    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaToHBase")
    // 配置Spark Streaming查询1s从kafka一个分区消费的最大的message数
    sparkConf.set("spark.streaming.maxRatePerPartition","100")
    // 优雅停止Spark
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //配置Kryo序列化方式
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator","com.ng.registrator.MyKryoRegistrator")

    //获取Streaming程序的入口
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("ck/kafka2HBase/kafka")

    //kafka 参数声明
    val topic = "log-analysis"
    val groupid = "g1"

    //kafka参数的封装
    val kafkaPara = Map(
      "bootstrap.servers" -> "psy831:9092,psy832:9092,psy833:9092",
      "group.id" -> groupid
    )

    //获取Offset
    val kafkaCluster = new KafkaCluster(kafkaPara)
    //读取上次消费的offset
    val partitionToLog:Map[TopicAndPartition,Long] = getOffset(kafkaCluster,groupid,topic)

    //读取kafka数据
    val kafkaStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaPara,
      partitionToLog,
      (mess: MessageAndMetadata[String, String]) => mess.message() //数据的处理方式
    )

    //过滤
    kafkaStream.filter(
      x => {
        if (x.contains("city")){
          true
        }else{
          false
        }
      }
    )

    //写入HBase
    kafkaStream.foreachRDD(rdd => {
      rdd.foreach(str => {
        println("*********")

        //将json数据转换为对象
        val logs: StartupReportLogs = JsonUtils.json2StartupLog(str)
        //取出城市和启动时间
        val city: String = logs.getCity
        println(city)
        //启动时间=
        val ts: lang.Long = logs.getStartTimeInMs
        //将时间转换为String
        val time: String = DateUtils.dateToString(new Date(ts))

        //拼接RowKey
        val rowKey: String = city+time
        //获取HBase表对象
        val table: Table = HBaseUtils.getHBaseTabel(properties)
        //向表中添加数据
        table.incrementColumnValue(Bytes.toBytes(rowKey),Bytes.toBytes("info"),
          Bytes.toBytes("count"),1L)
      })
    })
    //将offset提交
    setOffset(groupid,kafkaCluster,kafkaStream)

    ssc
  }

  def getOffset(kafkaCluster: KafkaCluster, groupid: String, topic: String): Map[TopicAndPartition, Long] = {

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

  def setOffset(groupid: String, kafkaCluster: KafkaCluster, kafkaStream: InputDStream[String]) = {
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
        val ack: Either[Err, Map[TopicAndPartition, Short]] = kafkaCluster.setConsumerOffsets(groupid, Map(topicAndPartition -> offset))

        if(ack.isLeft){
          println(s"Error:${ack.left.get}")
        }else{
          println(s"Success:${offsetRange.partition}:$offset")
        }
      }

    })
  }
}
