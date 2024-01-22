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
object Spark22_RDD_Oper_Transform_K_V_Require {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Requirement")
    val sc = new SparkContext(conf)

    //TODO 各省份广告被点击Top3
    // 时间戳 省份 城市 用户 广告
//    val top3 = sc.textFile("data/agent.log").
//      map(x => x.split(" ")).
//      groupBy(x => x(3)).
//      map(x => (x._1, x._2.size)).
//      sortBy(x => x._2, ascending = false).take(3)
//
//    top3.foreach(println)


    val rdd_a = sc.textFile("data/agent.log").groupBy(x => x.split(" ")(1))
    val rdd_b: RDD[(String, Unit)] = rdd_a.mapValues(list => {
      val datas: Iterable[(String, Int)] = list.map(x => (x.split(" ")(4), 1))
    })

    rdd_b.groupBy(_._1).
      mapValues(_.size).
      sortBy(x => x._2, ascending = false).
      take(3).foreach(println)


    sc.stop()
  }


}
