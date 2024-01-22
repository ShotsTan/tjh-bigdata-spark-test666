package com.tjh.bigdata.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * @ClassName: Spark01_Env
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/03 9:13
 * @Company: Copyright©
 * */
object Spark01_Env {
  def main(args: Array[String]): Unit = {

//    Source.fromFile("xxxxx.txt")
    //TODO 1.配置环境
    // (1).Scala不能直接读取本地或内存中的文件数据
    // (2).获取Spark框架连接对象读取数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountDemo01")

    val sc = new SparkContext(conf)


    sc.stop()



  }

}
