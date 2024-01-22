package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark15_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd1 = sc.parallelize(
      List(("nba", 1), ("nba", 2), ("nba", 3), ("nba", 4)), 2)

    //    rdd1.reduceByKey(_+_).collect().foreach(println)
    rdd1.reduceByKey({
      (x, y) => {
        println(x + "+" + y)
        x + y
      }
    }).collect().foreach(println)


    sc.stop()
  }


}
