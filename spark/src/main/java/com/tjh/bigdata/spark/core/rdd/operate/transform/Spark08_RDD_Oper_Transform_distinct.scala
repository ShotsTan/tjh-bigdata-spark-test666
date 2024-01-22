package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子 transform filter
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark08_RDD_Oper_Transform_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 过滤可能会产生数据倾斜
    val rdd = sc.parallelize(List(1,1,2, 2, 3, 4), 2)

    rdd.distinct().collect().foreach(println)

    sc.stop()
  }

}
