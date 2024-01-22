package com.tjh.bigdata.spark.core.rdd.instance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark03_RDD_Instance_Disk_Partitions_Data {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/word.txt", 2)
    val rdd2: RDD[String] = rdd1.flatMap(words => words.split(" "))
    rdd1.saveAsTextFile("output")

    rdd1.collect().foreach(println)

    sc.stop()
  }

}
