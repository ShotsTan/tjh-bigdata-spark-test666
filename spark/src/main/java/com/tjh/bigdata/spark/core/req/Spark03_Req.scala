package com.tjh.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Req
 * @Description: TODO Spark Core 案例
 * @Author: Tanjh
 * @Date: 2023/01/07 21:37
 * @Company: Copyright©
 * */
object Spark03_Req {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    // TODO 1.读取原始数据
    val rdd0: RDD[String] = sc.textFile("data/user_visit_action.txt")
    rdd0.persist()


    // TODO 2.统计分析
    //  2.1 统计点击数量

    rdd0.flatMap(
      data => {
        val datas = data.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        }else{
          Nil
        }
      }
    ).reduceByKey(
      (t1,t2) =>{
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    ).sortBy(_._2, ascending = false).take(10).foreach(println)

    // TODO 3.对数据排序，取前10名
    sc.stop()

  }

}
