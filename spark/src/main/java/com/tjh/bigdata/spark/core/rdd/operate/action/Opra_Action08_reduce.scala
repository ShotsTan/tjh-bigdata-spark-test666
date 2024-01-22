package com.tjh.bigdata.spark.core.rdd.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Opra_Action01
 * @Description: TODO 算子的分类
 * @Author: Tanjh
 * @Date: 2023/01/05 22:21
 * @Company: Copyright©
 * */
object Opra_Action08_reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action_01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    println(rdd1.reduce(_ + _))

    println(rdd1.aggregate(10)(_ + _, _ + _))

    println(rdd1.fold(10)(_ + _))

    sc.stop()
  }

}
