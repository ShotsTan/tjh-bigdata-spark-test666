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
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {


    //TODO 1.
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountDemo01")

    val sc = new SparkContext(conf)
    val fileDatas: RDD[String] = sc.textFile("data/word.txt")
    val wordDatas: RDD[String] = fileDatas.flatMap(_.split(" "))

    //TODO 将相同单词分在一起
    val wordGroup: RDD[(String, Iterable[String])] = wordDatas.groupBy(
      word => word
    )
    val wordCount: RDD[(String, Int)] = wordGroup.map(
      t => (t._1, t._2.size)
    )
//    wordCount.S
    // TODO 打印
    wordCount.collect().foreach(println)
//    wordGroup.mapValues()



    sc.stop()




  }

}
