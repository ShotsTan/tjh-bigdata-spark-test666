package com.tjh.bigdata.spark.streaming.once

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
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
 * */
object Once_Stard {
  def main(args: Array[String]): Unit = {

    //定义多久拉去一次数据
    val streamingContext = new StreamingContext("local[*]", "WordCountDemo", Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "tanjh01:9092,tanjh02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "220301",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val topicsA = Array("topicA")


    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsA, kafkaParams)
    )

    var ranges: Array[OffsetRange] = null
    val ds1: DStream[ConsumerRecord[String, String]] = ds.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val ds2: DStream[String] = ds1.map(record => record.value())

    ds2.foreachRDD(rdd => {
      rdd.foreach(word => println(Thread.currentThread().getName +":"+word))
      ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    })



    //    启动APP
    streamingContext.start()
    //  阻塞进程
    streamingContext.awaitTermination()
  }

}











