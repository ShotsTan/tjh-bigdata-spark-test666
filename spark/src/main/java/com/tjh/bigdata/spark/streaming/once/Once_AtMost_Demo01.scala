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
object Once_AtMost_Demo01 {
  def main(args: Array[String]): Unit = {

    //定义多久拉去一次数据
    val streamingContext = new StreamingContext("local[*]", "WordCountDemo", Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "tanjh01:9092,tanjh02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "220301",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true"
    )

    val topicsA = Array("topicA")


    val ds_A: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsA, kafkaParams)
    )




    ds_A.map(record => {
      Thread.sleep(500)
      if (record.value().equals("B")){
        throw new RuntimeException("程序异常")
      }
      record.value()
    }).print(1000)



    //    启动APP
    streamingContext.start()
    //  阻塞进程
    streamingContext.awaitTermination()
  }

}











