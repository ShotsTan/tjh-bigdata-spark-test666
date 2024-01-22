package com.tjh.bigdata.spark.core.rdd.persit

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Persit_01
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/06 22:01
 * @Company: Copyright©
 * */
object Persit02_CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Serial")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Fink"), 2)
    val rdd2: RDD[String] = rdd1.flatMap(x => {
      println("-----")
      x.split(" ")
    })
    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x, 1))
//    rdd3.cache()
    rdd3.cache()
    rdd3.checkpoint()


    val rdd4 = rdd3.reduceByKey(_ + _)
    rdd4.collect()//saveAsTextFile("output1")

    println("**" * 20)

    val rdd41: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd41.collect()//saveAsTextFile("output11")
  }

}
