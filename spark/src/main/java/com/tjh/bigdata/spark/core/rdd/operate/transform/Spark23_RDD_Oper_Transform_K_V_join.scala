package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO sortByKey
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark23_RDD_Oper_Transform_K_V_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Requirement")
    val sc = new SparkContext(conf)

    //TODO 各省份广告被点击Top3
    // 时间戳 省份 城市 用户 广告

    val rdd1 = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("f", 4)), 2)
    val rdd2 = sc.parallelize(List(("c", 1), ("a", 2), ("b", 3), ("d", 4)), 2)

    rdd1.join(rdd2).collect().foreach(println)
    sc.stop()
  }


}
