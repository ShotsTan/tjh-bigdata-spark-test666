package com.tjh.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL03_DF2DS {
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

    val ds: Dataset[User] = df.as[User]

    ds.show()





    sc.stop()
    spark.stop()
//    spark.close()
  }
  case class User(name:String,age:BigInt)

}
