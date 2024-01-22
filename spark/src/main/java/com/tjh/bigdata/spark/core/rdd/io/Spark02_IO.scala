package com.tjh.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_IO
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/06 23:22
 * @Company: Copyright©
 * */
object Spark02_IO {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Serial")
    val sc = new SparkContext(conf)

    println(sc.textFile("output1").collect().mkString("-"))
    println(sc.objectFile[(String,Int)]("output2").collect().mkString("-"))
    println(sc.sequenceFile[String,Int]("output3").collect().mkString("-"))


    sc.stop()
  }

}
