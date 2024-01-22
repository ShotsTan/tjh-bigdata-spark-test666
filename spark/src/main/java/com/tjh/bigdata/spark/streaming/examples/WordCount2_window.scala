package com.tjh.bigdata.spark.streaming.examples

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

 * */
object WordCount2_window {
  def main(args: Array[String]): Unit = {

    //定义多久拉去一次数据
    val streamingContext = new StreamingContext("local[*]", "WordCountDemo", Seconds(1))


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

//    val dss = ds.window(windowDuration = Seconds(10), slideDuration = Seconds(10))

    val ds1: DStream[String] = ds.map(record => record.value())
    val dss = ds1.window(windowDuration = Seconds(10), slideDuration = Seconds(10))

    val ds2: DStream[String] = dss.flatMap(line => line.split(" "))
    val ds3: DStream[(String, Int)] = ds2.map(word => (word, 1)).reduceByKey(_ + _)

    val ds4: DStream[(String, Int)] = ds3.transform(rdd => rdd.sortByKey(ascending = false))
    ds4.print(1000)



//    启动APP
    streamingContext.start()
//  阻塞进程
    streamingContext.awaitTermination()
  }

}











