package com.tjh.bigdata.spark.core.rdd.operate.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Opra_Action01
 * @Description: TODO 算子的分类
 * @Author: Tanjh
 * @Date: 2023/01/05 22:21
 * @Company: Copyright©
 * */
object Opra_Action09_reduce_wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action_01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),
      ("a", 1), ("b", 2), ("c", 3)), 2)

    val rdd2: RDD[mutable.Map[String, Int]] = rdd1.map(kv => mutable.Map(kv))

    val rdd3: mutable.Map[String, Int] = rdd2.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, cnt) => {
            val old: Int = map1.getOrElse(word, 0)
            map1.update(word, old + cnt)
          }
        }
        map1
      }
    )

    println(rdd3)
    sc.stop()
  }

}
