package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO 转换算子
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark11_RDD_Oper_Transform_sortBy_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    // 默认没有shuffle操作,加 true发送shuffle
    val rdd = sc.parallelize(
      List(
        User(12, 121, 112),
        User(32, 124, 122),
        User(52, 162, 127),
        User(13, 122, 120),
        User(22, 132, 10),
        User(42, 42, 121),

      ), 3)

    rdd.sortBy(a => (a.age, a.amount), ascending = false).collect().foreach(println)
    sc.stop()
  }

  //TODO 1.样例类： 可以直接应用在模式匹配中
  case class User(id: Int, age: Int, amount: Int)

}
