package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL11_DF_JSON {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "tjh")

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Req").
      enableHiveSupport().
      getOrCreate()

    spark.read.json("data/user.json").show()


    spark.stop()
  }


}
