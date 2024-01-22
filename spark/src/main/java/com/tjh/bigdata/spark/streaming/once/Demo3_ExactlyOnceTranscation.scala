package com.tjh.bigdata.spark.streaming.once

import com.tjh.bigdata.spark.streaming.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable

/**
 * @ClassName: Demo3_ExactlyOnceTranscation
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/03/03 20:52
 * @Company: Copyright©
 * */
object Demo3_ExactlyOnceTranscation {


  val groupid = "220101"
  val topic = "topicA"

  //获取偏移量
  def selectOffectsFromMySQL(groupid: String, topic: String): Map[TopicPartition, Long] = {
    val offset = new mutable.HashMap[TopicPartition, Long]()
    val sql =
      """
        |select
        |*
        |from offsets
        |where groupid = ? and topic = ?
        |
        |""".stripMargin
    var conn: Connection = null
    var ps: PreparedStatement = null

    try {
      conn = JDBCUtil.getConnection()
      ps = conn.prepareStatement(sql)
      ps.setString(1, groupid)
      ps.setString(2, topic)

      val resultSet: ResultSet = ps.executeQuery()
      while(resultSet.next()){
        offset.put(new TopicPartition(topic,resultSet.getInt("partitionId")),resultSet.getLong("offset"))
      }
    } catch {
      case e:Exception => {
        e.printStackTrace()
        throw new RuntimeException("查询偏移量失败")
      }
    } finally {
      if (ps != null){
        ps.close()
      }
      if(conn!= null){
        conn.close()
      }
    }

    offset.toMap
  }

  //事务写入MySQL
  def whileResultAndOffsetsInCommonTransaction(results: Array[(String, Int)], ranges: Array[OffsetRange]): Unit = {
    //写单词
    val sql1=
      """
        |insert into wordcount values(?,?)
        |on duplicate key update count = count+values(count)
        |
        |""".stripMargin
//    写偏移量
    val sql2 =
      """
        |insert into offsets values(?,?,?,?)
        |on duplicate key update offset = values(offset)
        |
        |""".stripMargin

    var connection: Connection = JDBCUtil.getConnection()
    var ps1: PreparedStatement = null
    var ps2: PreparedStatement = null




    try {
      connection = JDBCUtil.getConnection()
      connection.setAutoCommit(false)

      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)

      for ((word,count) <- results) {
        ps1.setString(1, word)
        ps1.setLong(2, count)
        ps1.addBatch()
      }

      for (offsetsRange <- ranges) {
        ps2.setString(1, groupid)
        ps2.setString(2, topic)
        ps2.setInt(3, offsetsRange.partition)
        ps2.setLong(4, offsetsRange.fromOffset)

        ps2.addBatch()
      }


      val res1: Array[Int] = ps1.executeBatch()
      val res2: Array[Int] = ps2.executeBatch()
      connection.commit()

      println("数据写入"+res1.size)
      println("偏移量写入"+res2.size)


    } catch {
      case e: Exception => {
        connection.rollback()
        e.printStackTrace()
        throw new RuntimeException("写入失败")
      }
    } finally {
      if (ps1 != null) {
        ps1.close()
      }
      if (ps2 != null) {
        ps2.close()
      }
      if (connection != null) {
        connection.close()
      }
    }


  }

  def main(args: Array[String]): Unit = {
    //1.查询偏移量
    val offsetMap: Map[TopicPartition, Long] = selectOffectsFromMySQL(groupid, topic)

    val streamingContext = new StreamingContext("local[*]", "TransformDemo", Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "tanjh01:9092,tanjh02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topicsA = Array(topic)

    //2.从MySQL查的偏移量向后消费
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsA, kafkaParams)
    )
    ds.foreachRDD(rdd => {

      if (!rdd.isEmpty()){
        //3.获取偏移量  ---Driver端运行
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //4.转换运算  --Executor运算
        val results: Array[(String, Int)] = rdd.flatMap(record => record.value().split(" ")).
          map(word => (word, 1)).reduceByKey(_ + _).collect()


        //5. 将result和ranges在一个事务中写出
        whileResultAndOffsetsInCommonTransaction(results, ranges)
      }

    })


    streamingContext.start()
    //  阻塞进程
    streamingContext.awaitTermination()


  }


}
