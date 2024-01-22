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
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    //TODO 1.
    val conf = new SparkConf().setMaster("yarn").setAppName("WordCount!")
    //TODO 可将sc理解为Driver
    val sc = new SparkContext(conf)


    // linux: file:///opt/module/xxx
    // windows: file://c:/test/test
    val fileDatas: RDD[String] = sc.textFile("data/word.txt")

    val wordDatas: RDD[String] = fileDatas.flatMap(_.split(" "))

    //TODO 将相同单词分在一起
    val wordGroup: RDD[(String, Iterable[String])] = wordDatas.groupBy(
      word => word
    )
    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, wordList) => (word, wordList.size)
    }
    //    wordGroup.mapValues(_.size)
    // TODO 打印
    wordCount.collect().foreach(println)


    sc.stop()


  }

}
