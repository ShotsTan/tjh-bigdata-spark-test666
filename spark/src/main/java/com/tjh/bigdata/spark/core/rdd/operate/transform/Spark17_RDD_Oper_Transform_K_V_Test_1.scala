package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark17_RDD_Oper_Transform_K_V_Test_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3), ("a", 4), ("b", 5), ("a", 6)), 2)

    //TODO 求每个分区最大值的和 aggregateByKey:第一个参数表示相同key分区内计算，第二个表示相同key分区间计算
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)



    sc.stop()
  }


}
