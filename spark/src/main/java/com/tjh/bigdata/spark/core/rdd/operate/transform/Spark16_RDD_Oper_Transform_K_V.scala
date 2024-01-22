package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark16_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd1 = sc.parallelize(
      List(("cba", 1), ("nba", 2), ("cba", 3), ("nba", 4)), 2)
    // TODO 自定义规则分组
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(x => x._1)

    val rdd3: RDD[(String, Iterable[Int])] = rdd1.groupByKey()

    rdd1.groupByKey().collect().foreach(println)

    rdd1.reduceByKey(_+_)

    sc.stop()
  }


}
