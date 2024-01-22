package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO sortByKey
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark20_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("b", 5), ("a", 6)), 2)
    rdd.sortByKey(false).collect().foreach(println)

    rdd.mapValues(x=>x+100).collect().foreach(println)

    sc.stop()
  }


}
