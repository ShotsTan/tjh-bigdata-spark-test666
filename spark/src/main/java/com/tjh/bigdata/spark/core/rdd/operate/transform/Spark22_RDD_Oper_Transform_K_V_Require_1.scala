package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO sortByKey
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark22_RDD_Oper_Transform_K_V_Require_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Requirement")
    val sc = new SparkContext(conf)

    //TODO 各省份广告被点击Top3
    // 时间戳 省份 城市 用户 广告

    val lineRDD: RDD[String] = sc.textFile("data/agent.log")
    val reduceRDD: RDD[((String, String), Int)] = lineRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    ).reduceByKey(_ + _)

    val mapRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map {
      case ((prv, ad), cnt) => (prv, (ad, cnt))
    }.groupByKey()

    val top3RDD: RDD[(String, List[(String, Int)])] = mapRDD.
      mapValues(iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).
        take(3))

    top3RDD.collect().foreach(println)


    sc.stop()
  }


}
