package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform RDD
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark01_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    //TODO RDD分类:
    // 1.transform : 实现功能的组合和拓展 :
    // (1).单值类型 : List(1,2,3,4)、
    // (2).双值类型 : List(1,2,3,4),List(1,2,3,4)
    // (3).键值类型 : List((1:"a"),(2,"b"))
    // 2.action ：用于执行RDD的操作

    sc.stop()
  }

}
