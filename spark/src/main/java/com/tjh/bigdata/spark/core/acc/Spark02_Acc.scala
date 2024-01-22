package com.tjh.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Acc
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/08 0:16
 * @Company: Copyright©
 * */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    // 声明累加器模型
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val collect: CollectionAccumulator[Nothing] = sc.collectionAccumulator("collect")


    rdd.foreach(
      num => {
        sum.add(num)
        println(s"sum = ${sum}")
      }

    )
    println(sum.value)




    sc.stop()

  }
}
