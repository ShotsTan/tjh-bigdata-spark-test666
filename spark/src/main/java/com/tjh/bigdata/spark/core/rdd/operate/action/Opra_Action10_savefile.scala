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
object Opra_Action10_savefile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action_01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),
      ("a", 1), ("b", 2), ("c", 3)), 2)

    rdd1.saveAsTextFile("output1")

    rdd1.saveAsObjectFile("output2")
    //必须K-V键值对类型数据
    rdd1.saveAsSequenceFile("output3")

    //    println(rdd3)
    sc.stop()
  }

}
