package com.tjh.bigdata.spark.core.rdd.operate.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Opra_Action01
 * @Description: TODO 算子的分类
 * @Author: Tanjh
 * @Date: 2023/01/05 22:21
 * @Company: Copyright©
 * */
object Opra_Action07_countByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action_01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("nba", 1), ("cba", 2), ("nba", 1), ("nba", 4)), 2)
//        rdd1.flatMap {
//          case (word,cnt)=>{
//            val list:(String,Int) = List()
//            for (i <- 0 to cnt){
//              list+(word,1)
//            }
//            list
//          }
//        }

    val map: collection.Map[String, Long] = rdd1.countByKey()
    println(map)

    val map1: collection.Map[(String, Int), Long] = rdd1.countByValue()
    println(map1)

    sc.stop()
  }

}
