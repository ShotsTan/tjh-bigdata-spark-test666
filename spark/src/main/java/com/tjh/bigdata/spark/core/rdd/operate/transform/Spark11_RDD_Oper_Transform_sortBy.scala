package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark11_RDD_Oper_Transform_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd = sc.parallelize(List(9, 6, 8, 4, 3, 2, 1, 7, 4), 3)

    rdd.sortBy(a => a % 2, ascending = false).collect().foreach(println)
    sc.stop()
  }

}
