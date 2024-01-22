package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL06_DF_UDF {
  def main(args: Array[String]): Unit = {

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Demo").
      getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    //    val rdd = sc.makeRDD(List(
    //      (1,"zhangsan",30), (2,"lisi",68), (3,"wangwu",66)
    //    ))

    val df: DataFrame = spark.read.json("data/user.json")
    //TODO RDD->DS  DF更侧重数据和结构 DS更侧重类型

    df.createTempView("user")



    spark.udf.register("prefixName",
      (name:String) => "Name:"+name)

    spark.sql(
      """
        |select prefixName(name) from user
        |""".stripMargin).show()


    sc.stop()
    spark.stop()
  }

  case class User(name: String, age: BigInt)

}
