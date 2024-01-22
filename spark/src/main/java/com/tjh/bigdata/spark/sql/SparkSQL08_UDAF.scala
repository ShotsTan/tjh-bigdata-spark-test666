package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL08_UDAF {
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


    spark.udf.register("avgAge", new MyAvgAgeUDAF())

    spark.sql("select avgAge(age) from user").show()


    sc.stop()
    spark.stop()
  }

  //旧版本udaf
  class MyAvgAgeUDAF extends UserDefinedAggregateFunction {
    //输入数据结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)))
    }

    //缓冲区数据结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("cnt", LongType),
          StructField("total", LongType)
        )
      )
    }

    //输出数据结构
    override def dataType: DataType = {
      LongType
    }

    //计算稳定性
    override def deterministic: Boolean = true

    //初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    //更新
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + 1)
      buffer.update(1, buffer.getLong(1) + input.getLong(0))

    }

    //合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    //计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(1) / buffer.getLong(0)
    }
  }


}
