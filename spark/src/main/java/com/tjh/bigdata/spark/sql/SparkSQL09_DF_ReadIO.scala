package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL09_DF_ReadIO {
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

    val df: DataFrame = spark.read.format("json").load("data/user.json")
//    spark.read.parquet()
//    spark.read.orc()
    df.show()

    sc.stop()
    spark.stop()
  }


}
