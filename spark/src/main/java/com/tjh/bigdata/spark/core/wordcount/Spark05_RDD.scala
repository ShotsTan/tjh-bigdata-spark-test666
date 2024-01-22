package com.tjh.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Env
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 9:13
 * @Company: Copyright©
 * */
object Spark05_RDD {
  def main(args: Array[String]): Unit = {


    //TODO 1.
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount!")

    //TODO 可将sc理解为Driver
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")


    val value: RDD[(String, Int)] = sc.textFile("data/word.txt").flatMap(_.split(" ")).map(word => (word, 1)).
      reduceByKey(_ + _).sortBy({case (word,cnt) => cnt},ascending = false)
    value.foreach(println)


    sc.stop()


  }

}
