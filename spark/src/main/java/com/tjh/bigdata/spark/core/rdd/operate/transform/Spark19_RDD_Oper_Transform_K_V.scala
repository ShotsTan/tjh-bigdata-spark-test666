package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO combineByKey
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark19_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3), ("a", 4), ("b", 5), ("a", 6)), 2)

    //    rdd.combineByKey()
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(x=>x,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)


    sc.stop()
  }


}
