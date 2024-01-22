package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform RDD
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark06_RDD_Oper_Transform_Shuffle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd11 = sc.parallelize(List(1,2,3,4),3)
    rdd11.groupBy(_ % 2,2).saveAsTextFile("output")

    rdd11.map((_,1)).reduceByKey(_+_).collect().foreach(println)


    sc.stop()
  }

}
