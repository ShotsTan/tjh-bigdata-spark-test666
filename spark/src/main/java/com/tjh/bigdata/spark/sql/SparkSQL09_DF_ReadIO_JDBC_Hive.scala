package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL09_DF_ReadIO_JDBC_Hive {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "tjh")

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      enableHiveSupport().
      getOrCreate()

    ///data/spark/data/sparksql/  city_info.txt  product_info.txt 	user_visit_action.txt

    spark.sql("show databases").show()

    spark.stop()
  }


}
