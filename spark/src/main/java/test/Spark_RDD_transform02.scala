package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @ClassName: Spark_RDD_transform01
 * @Description: TODO RDD transform算子练习 K-V 类型
 * @Author: Tanjh
 * @Date: 2023/01/09 14:12
 * @Company: Copyright©
 * */
object Spark_RDD_transform02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("Spark_RDD_Transform_02")
//      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(conf)

    //    val rdd0: RDD[String] = sc.textFile("data/user_visit_action.txt")
    val rdd0: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("e", 1), ("c", 3), ("a", 1), ("b", 4), ("a", 7), ("c", 2), ("c", 1)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 34), ("c", 3), ("b", 4), ("a", 7), ("c", 2), ("c", 1)), 2)

    //TODO 3.Key - Value 类型
    //1.partitionBy 按**分区，需要new 分区器 HashPartitioner --
    println(rdd0.partitionBy(new HashPartitioner(2)).getNumPartitions)


    //2.groupByKey
    println(rdd1.groupByKey().collect().toList)
    println(rdd0.groupByKey(2).collect().toList)


    //3.reduceByKey
    println(rdd0.reduceByKey((x, y) => x).collect().toList)
    println(rdd1.reduceByKey(_ + _).collect().toList)

    println("---------------------")
    //4.aggregateByKey
    println(rdd1.aggregateByKey(0)((x, y) => {
      Math.max(x, y)
    },
      _ + _).collect().toList)


    //5.foldByKey
    println(rdd1.foldByKey(0)(_ + _).collect().toList)


    //6.combineByKey
    println(rdd1.combineByKey(x => (x, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc1._2)
    ).map {
      case (x, (y, z)) => (x, y / z)
    }.collect().toList)



    //7.sortByKey
    println(rdd1.sortByKey(ascending = false).collect().toList)


    //8.join

    println(rdd1.join(rdd2).collect().toList)


    //9.leftOuterJoin
    println(rdd1.leftOuterJoin(rdd2).map(x => (x._1, (x._2._1, x._2._2.getOrElse()))).collect().toList)


    //10.cogroup
    println(rdd1.cogroup(rdd2).map(x => (x._1, (x._2._1.mkString(",") + "," + x._2._2.mkString(",")))).collect().toList)

    val rdd_co: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    val list1: List[(String, List[Int])] = rdd_co.map {
      case (x, (a, b)) => (x, a.toList.union(b.toList))
    }.collect().toList

    println(list1)

    sc.stop()
  }

}
