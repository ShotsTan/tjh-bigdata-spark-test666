package com.tjh.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * @ClassName: Spark01_Env
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 9:13
 * @Company: Copyright©
 * */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {


    //TODO 1.
    val conf = new SparkConf().setMaster("yarn").setAppName("WordCount!")

    //TODO 可将sc理解为Driver
    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")


    sc.textFile("data/word.txt").flatMap(_.split(" ")).map(word => (word, 1)).
      reduceByKey(_ + _).foreach(println)


    sc.stop()


  }

}
