package com.tjh.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName: Spark01_Acc
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/08 0:16
 * @Company: Copyright©
 * */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    var sum = 0




    rdd.foreach(
      num => {
        sum += num
        println(s"sum = ${sum}")
      }

    )
    println(sum)




    sc.stop()

  }
}
