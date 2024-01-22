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
object Spark12_RDD_Oper_Transform_doubleValue_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd1 = sc.parallelize(
      List(1, 2, 3, 4, 11, 0, 8), 3)
    val rdd2 = sc.parallelize(
      List(7, 9, 3, 4, 5,7,9), 3)

    //TODO 1.交集
    println(rdd1.intersection(rdd2).collect().mkString(","))

    //TODO 2.并集
    println(rdd1.union(rdd2).collect().mkString(","))
    println(rdd1.union(rdd2).distinct().collect().mkString(","))
    //TODO 3.差集
    println(rdd1.subtract(rdd2).collect().mkString(","))
    println(rdd2.subtract(rdd1).collect().mkString(","))

    //TODO 4.拉链 zip
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd3.zip(rdd1).collect().mkString(","))

    sc.stop()
  }


}
