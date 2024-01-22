package com.tjh.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL04_RDD2DS {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val value: RDD[(Int, String, Int)] = sc.makeRDD(List(
      (1, "zhangsan", 30), (2, "lisi", 68), (3, "wangwu", 66)
    ))
    val rdd = value

    //TODO RDD->DS
    val ds: Dataset[User] = rdd.map {
      case (id, name, age) =>
        User(id, name, age)
    }.toDS()

    rdd.toDS().show()

    ds.toDF().rdd.collect().foreach(println)

    ds.show()

    ds.rdd.collect().foreach(println)





    spark.stop()


  }
  case class User(id:Int,name:String,age:Int)

}
