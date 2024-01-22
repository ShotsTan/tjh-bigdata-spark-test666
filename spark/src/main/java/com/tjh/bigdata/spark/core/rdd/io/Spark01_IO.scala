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
object Spark01_IO {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Serial")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)


    rdd1.saveAsTextFile("output1")
    rdd1.saveAsObjectFile("output2")
    rdd1.saveAsSequenceFile("output3")

//    rdd1.toDF

//    rdd1.collect()

    sc.stop()
  }

}
