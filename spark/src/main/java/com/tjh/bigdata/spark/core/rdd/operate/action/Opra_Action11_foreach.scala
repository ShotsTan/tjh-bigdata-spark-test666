package com.tjh.bigdata.spark.core.rdd.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Opra_Action01
 * @Description: TODO 算子的分类
 * @Author: Tanjh
 * @Date: 2023/01/05 22:21
 * @Company: Copyright©
 * */
object Opra_Action11_foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action_01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)

    val user = new User()

    rdd1.foreach(num => {
      println(user.age + num)
    })

    //算子内部代码运行在executor中，
    //算子外部代码运行在driver
    //    println(rdd3)
    sc.stop()
  }

  //  case class User extends Serializable {
  case class User() {
    val age = 30
  }
}
