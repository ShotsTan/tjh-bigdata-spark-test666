package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform filter
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark10_RDD_Oper_Transform_repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd = sc.parallelize(List(1, 1, 2, 2, 3, 4), 3)

    // 增加分区。一般使用repartition 缩减分区一般使用coalesce()
//    rdd.coalesce(5,true).saveAsTextFile("output")
    rdd.repartition(6).saveAsTextFile("output")
    sc.stop()
  }

}
