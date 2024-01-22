package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL07_DF_UDAF {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    val sc = spark.sparkContext

    //    val rdd = sc.makeRDD(List(
    //      (1,"zhangsan",30), (2,"lisi",68), (3,"wangwu",66)
    //    ))

    val df: DataFrame = spark.read.json("data/user.json")
    //TODO RDD->DS  DF更侧重数据和结构 DS更侧重类型

    df.createTempView("user")


    spark.udf.register("avgAge", functions.udaf(new MyAvgAgeUDAF())
    )

    spark.sql(
      """
        |select avgAge(age) from user
        |""".stripMargin).show()

    //    df.select("myavgageudaf(age)").show()


    sc.stop()
    spark.stop()
  }

  case class User(name: String, age: BigInt)

  case class AvgAgeBuffer(var total: Long, var cnt: Int)

  class MyAvgAgeUDAF extends Aggregator[Long, AvgAgeBuffer, Long] {
    //初始值
    override def zero: AvgAgeBuffer = {
      AvgAgeBuffer(0L, 0)
    }

    //分区内计算
    override def reduce(buff: AvgAgeBuffer, age: Long): AvgAgeBuffer = {
      AvgAgeBuffer(buff.total + age, buff.cnt + 1)
    }

    //分区间计算
    override def merge(b1: AvgAgeBuffer, b2: AvgAgeBuffer): AvgAgeBuffer = {
      AvgAgeBuffer(b1.total + b2.total, b1.cnt + b2.cnt)
    }

    //    结果
    override def finish(reduction: AvgAgeBuffer): Long = {
      reduction.total / reduction.cnt
    }

    //固定写法
    override def bufferEncoder: Encoder[AvgAgeBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
