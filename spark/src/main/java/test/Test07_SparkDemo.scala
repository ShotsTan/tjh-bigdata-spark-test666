package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Test07_SparkDemo
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/29 21:51
 * @Company: Copyright©
 * */
object Test07_SparkDemo {
  def main(args: Array[String]): Unit = {
//    val spark = new SparkSession.Builder().master("local[*]").appName("demo").getOrCreate()

    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("D:\\tmp\\Scala_Project\\tjh-bigdata-spark\\data\\word.txt")
    rdd1.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,t)=>x+t).sortBy((x)=> x._2,ascending = false).collect().foreach(println)

    sc.stop()


  }

}
