package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子一般不会转换 RDD分区个数
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark02_RDD_Oper_Transform_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    //TODO RDD分类:
    // 1.transform : 实现功能的组合和拓展 :
    // (1).map()    单值类型 : List(1,2,3,4)、
    val rdd1 = sc.parallelize(List(1, 2, 3, 4), 2)
    // map需要传递一个参数:
    val rdd2: RDD[Int] = rdd1.map(data => {
      println("sad====>", data)
      data * 2

    })
    //    val rdd2: RDD[Int] = rdd1.map(_ * 2)

    rdd2.collect().foreach(println)
    //    rdd2.saveAsTextFile("output")

    println(rdd2.getNumPartitions)


    sc.stop()
  }

}
