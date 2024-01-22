package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL01_DF {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    val sc = spark.sparkContext

    val df: DataFrame = spark.read.json("data/user.json")

    df.show()

    df.createTempView("user")

    spark.sql(
      """
        |select name,age from user where age > 30
        |""".stripMargin).show()

    df.select("name", "age").where("age>30").show()

    df.printSchema()


    spark.stop()


  }

}
