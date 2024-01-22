package com.tjh.bigdata.spark.core.rdd.instance

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark01_RDD_Instance {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)
    println("saf".charAt(0))
    println("swecf"(0))

    //TODO 构建RDD 只能通过
    // -- 1.内存集合: parallelize()
    // -- 2.磁盘文件:textFile()
    // -- 3.继承其他 RDD

    sc.stop()
  }

}
