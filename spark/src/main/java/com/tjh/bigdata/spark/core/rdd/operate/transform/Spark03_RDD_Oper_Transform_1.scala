package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform RDD
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark03_RDD_Oper_Transform_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1, 2, 3, 4),2)

    // TODO mapPartitions : 用于将一个分区内数据转换
    val rdd2: RDD[Int] = rdd1.mapPartitions(
      iter => iter.map(_ * 2)
    )

    rdd1.mapPartitionsWithIndex(
      (ind,iter) => {
        if (ind == 1){
          iter
        }else{
          Nil.iterator
        }
      }
    ).collect().foreach(println)

//    rdd2.collect().foreach(println)

    sc.stop()
  }

}
