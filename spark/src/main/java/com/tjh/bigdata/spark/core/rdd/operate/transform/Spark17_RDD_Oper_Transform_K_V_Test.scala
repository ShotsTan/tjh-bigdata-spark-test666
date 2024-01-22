package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark17_RDD_Oper_Transform_K_V_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5,6,7),3)

    //TODO 求每个分区最大值的和
    rdd.mapPartitions(iter => List(iter.max).iterator).map(x=>(1,x)).reduceByKey(_+_).map(_._2).collect().foreach(println)
    rdd.mapPartitions(iter => List(iter.max).iterator).map(x=>(1,x)).groupByKey(1).map(x => x._2.sum).collect().foreach(println)


    sc.stop()
  }


}
