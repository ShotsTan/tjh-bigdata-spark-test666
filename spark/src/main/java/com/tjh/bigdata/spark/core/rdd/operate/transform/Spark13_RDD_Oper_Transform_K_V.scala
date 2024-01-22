package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark13_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd1: RDD[Int] = sc.parallelize(
      List(97, 100, 103, 104, 107, 99, 108), 3)
    val rdd2 = rdd1.map(x => (x.toChar.toString,1))
    rdd2.collect().foreach(println)

    // RangePartitioner  HashPartitioner
    rdd2.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

//    rdd2.partitionBy(new RangePartitioner()).saveAsTextFile("output")


    sc.stop()
  }


}
