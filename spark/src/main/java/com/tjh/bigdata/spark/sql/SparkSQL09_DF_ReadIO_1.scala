package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL09_DF_ReadIO_1 {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    val sc = spark.sparkContext

    val df: DataFrame = spark.read.format("json").load("data/user.json")

    df.show()
    //保存数据

//    df.write.mode("append").save("output")
    df.write.format("json").mode("append").save("data/a.json")

    sc.stop()
    spark.stop()
  }


}
