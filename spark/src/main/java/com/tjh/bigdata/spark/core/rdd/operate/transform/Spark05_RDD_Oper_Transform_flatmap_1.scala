package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform RDD
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark05_RDD_Oper_Transform_flatmap_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List("as adc", 12, "ascdc sdcfa", List(3, 6, 9)))

    rdd1.flatMap {
      case list: List[String] => list
      case str: String => str.split(" ")
      case num: Int => List(num)
    }.collect().foreach(println)

    //    rdd2.collect().foreach(println)
    sc.stop()
  }

}
