package com.tjh.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Env
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 9:13
 * @Company: Copyright©
 * */
object Spark06_WordCount_10 {
  def main(args: Array[String]): Unit = {


    //TODO 1.
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount!")

    //TODO 可将sc理解为Driver
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(List("a","a","a","b","b"),2)

    // TODO 1. groupBy
    val groupRDD: RDD[(String, Iterable[String])] = rdd.groupBy(w => w)
    val wordCount1: RDD[(String, Int)] = groupRDD.map(x => (x._1, x._2.size))

    // TODO 2.countByValue
    val WordCount2: collection.Map[String, Long] = rdd.countByValue()

    // TODO 3. reduceByKey
    val WD3: RDD[(String, Int)] = rdd.map(x => (x, 1)).reduceByKey(_ + _)

    // TODO 4.groupByKey
    val WD4: RDD[(String, Int)] = rdd.map(x => (x, 1)).groupByKey().mapValues(_.size)

    // TODO 5.countByKey
    val WD5: collection.Map[String, Long] = rdd.map(x => (x, 1)).countByKey()

    // TODO 6.aggregateByKey
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("b", 2), ("a", 3), ("a", 3)))
    val WD6: RDD[(String, Int)] = rdd1.aggregateByKey(0)(_ + _, _ + _)

    // TODO 7.foldByKey
    val WD7: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)

    // TODO 8.combineByKey
    rdd1.combineByKey(
      x=>x,
      (v1:Int,v2:Int)=>{
        v1+v2
      },
      (v1:Int,v2:Int)=>{
        v1+v2
      }
    )

    // TODO 8.fold


    // TODO 8.reduce

    // TODO 8.aggregate




    sc.stop()


  }

}
