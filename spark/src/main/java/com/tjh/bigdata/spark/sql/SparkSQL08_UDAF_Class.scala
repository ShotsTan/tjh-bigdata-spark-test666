package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL08_UDAF_Class {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._



    val df: DataFrame = spark.read.json("data/user.json")
    //TODO RDD->DS  DF更侧重数据和结构 DS更侧重类型
    val ds: Dataset[User] = df.as[User]

    val udaf = new MyAvgAge()

    ds.select(udaf.toColumn).show()




    sc.stop()
    spark.stop()
  }

  case class User(name:String,age:Long)
  case class AvgAgeBuffer(var total:Long,var cnt:Int)


  class MyAvgAge extends Aggregator[User,AvgAgeBuffer,Long]{
    override def zero: AvgAgeBuffer = {
      AvgAgeBuffer(0L,0)
    }

    override def reduce(b: AvgAgeBuffer, a: User): AvgAgeBuffer = {
      AvgAgeBuffer(b.total+a.age,b.cnt+1)
    }

    override def merge(b1: AvgAgeBuffer, b2: AvgAgeBuffer): AvgAgeBuffer = {
      AvgAgeBuffer(b1.total+b2.total,b1.cnt+b2.cnt)
    }

    override def finish(reduction: AvgAgeBuffer): Long = {
      reduction.total/reduction.cnt
    }


    override def bufferEncoder: Encoder[AvgAgeBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
