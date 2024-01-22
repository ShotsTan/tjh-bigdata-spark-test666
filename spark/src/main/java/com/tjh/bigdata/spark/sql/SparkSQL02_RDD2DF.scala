package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL02_RDD2DF {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val rdd = sc.makeRDD(List(
      (1,"zhangsan",30), (2,"lisi",68), (3,"wangwu",66)
    ))

    //TODO RDD->DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    df.show()


    spark.stop()


  }

}
