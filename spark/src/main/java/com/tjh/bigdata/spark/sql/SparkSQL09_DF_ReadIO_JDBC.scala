package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL09_DF_ReadIO_JDBC {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    val sc = spark.sparkContext

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://tanjh01:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "zxcvbnm,./")
      .option("dbtable", "base_attr_info")
      .load()

    df.createTempView("base_attr_info")
    df.show(3)

    spark.sql("select * from base_attr_info").write.format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", "jdbc:mysql://tanjh01:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "zxcvbnm,./")
      .option("dbtable", "base_attr_info_01")
      .save()

    sc.stop()
    spark.stop()
  }


}
