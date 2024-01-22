package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark14_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd1 = sc.parallelize(
      List(("nba", 1), ("cba", 2), ("nba", 3), ("cba", 4), ("cca", 5)), 2)



    // RangePartitioner  HashPartitioner,自定义分区规则
    rdd1.partitionBy(new Mypartitioner).saveAsTextFile("output")

    sc.stop()
  }

  //TODO 自定义分区器
  class Mypartitioner extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cca" => 1
        case "cba" => 2
      }
    }
  }


}
