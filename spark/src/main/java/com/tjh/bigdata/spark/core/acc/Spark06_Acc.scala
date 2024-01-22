package com.tjh.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Spark01_Acc
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/08 0:16
 * @Company: Copyright©
 * */
object Spark06_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2)), 3)
    val rdd2 = sc.makeRDD(List(("a", 3), ("b", 4)), 3)


    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val map = Map(("a", 3), ("b", 4))
    val bc: Broadcast[Map[String, Int]] = sc.broadcast(map)
    rdd1.map {
      case (word, cnt) => {
        (word, (cnt, bc.value.getOrElse(word, 0)))
      }
    }.foreach(println)

    sc.stop()

  }
}
