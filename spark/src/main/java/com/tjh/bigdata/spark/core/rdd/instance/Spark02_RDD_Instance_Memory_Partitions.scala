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
object Spark02_RDD_Instance_Memory_Partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    //TODO 构建RDD 只能通过 1.内存集合: parallelize()
    val seq = Seq(1, 2, 3, 4,5)
    val rdd1: RDD[Int] = sc.parallelize(seq,3)
    List
    // makeRDD 默认调用 parallelize()
    sc.makeRDD(seq)

    val rdd2: RDD[Int] = rdd1.map(num => num * num)
    rdd1.saveAsTextFile("output")

    rdd2.collect().foreach(println)



    sc.stop()
  }

}
