package com.tjh.bigdata.spark.streaming.examples

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: WordCount
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/02/27 22:14
 * @Company: Copyright©
 *
 *
 * 编程入口:
 *        SparkCore       : SparkContext
 *        SparkSQL       :  SparkSession(SparkContext)
 *        SparkStreaming : StreamingContext(SparkContext)
 *
 *  编程模型 :
 *        SparkCore       : RDD
 *        SparkSQL       :  DataFream(SparkContext)
 *        SparkStreaming : DataStream(SparkContext)
 *
 *        1.创建StreamingContext
 *        2.创建DStream
 *        3.调用DStream算子
 *        4.启动APP -> StreamingContext.start() -> StreamingContext.stop()
 *        ->StreamingContext.awaitTermination()
 *        batchDuration:
 *              Milliseconds -> 毫秒
 *              Seconds -> 秒
 *              Minutes -> 分
 *
 *
 * */
object WordCount {
  def main(args: Array[String]): Unit = {
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo")
//    val streamingContext = new StreamingContext(conf = sparkConf, batchDuration = Minutes(1))

    val streamingContext = new StreamingContext("local[*]", "WordCountDemo", Seconds(1))
    val sparkContext: SparkContext = streamingContext.sparkContext
    val dstream: InputDStream[(Nothing, Nothing)] = streamingContext.fileStream("D:\\tmp\\Scala_Project\\tjh-bigdata-spark\\data\\aaa.txt")
//    streamingContext.socketStream("监听端口")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "tanjh01:9092,tanjh02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "220309",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA")

    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val ds1: DStream[String] = ds.map(record => record.value())
    val ds2: DStream[String] = ds1.flatMap(line => line.split(" "))
    val ds3: DStream[(String, Int)] = ds2.map(word => (word, 1)).reduceByKey(_ + _)

    val ds4: DStream[(String, Int)] = ds3.transform(rdd => rdd.sortByKey(ascending = false))
    ds4.print(1000)

//    ds3.transform()

//    启动APP
    streamingContext.start()
//  阻塞进程
    streamingContext.awaitTermination()
  }

}











