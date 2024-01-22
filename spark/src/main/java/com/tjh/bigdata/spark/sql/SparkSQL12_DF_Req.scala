package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO SparkSQL案例
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL12_DF_Req {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "tjh")

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Req").
      enableHiveSupport().
      getOrCreate()

    ///data/spark/data/sparksql/  city_info.txt  product_info.txt 	user_visit_action.txt

    spark.sql("use spark_01")
    spark.sql(
      """
        |select ac.*,
        |                         city_name,
        |                         area,
        |                         product_name
        |                  from user_visit_action ac
        |                           join city_info ci
        |                                on ac.city_id = ci.city_id
        |                           join product_info pi
        |                                on product_id = ac.click_product_id
        |                  where click_product_id != -1
        |""".stripMargin).createTempView("t1")
    spark.udf.register("cityRemack", functions.udaf(new CityRemackUDAF()))

    spark.sql(
      """
        |select area,
        |                   product_name,
        |                   count(1) as cnt,
        |                   cityRemack(city_name) as `cityRemack`
        |            from t1
        |            group by area, product_name
        |""".stripMargin).createTempView("t2")
    spark.sql(
      """
        |select *,
        |             rank() over (partition by area order by cnt DESC ) as `rank`
        |      from t2
        |""".stripMargin).createTempView("t3")

    spark.sql(
      """
        |select *
        |from t3
        |where rank <= 3 order by area;
        |""".stripMargin).show(truncate = false)


    spark.stop()
  }

  case class CityBuffer(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityRemackUDAF extends Aggregator[String, CityBuffer, String] {
    override def zero: CityBuffer = CityBuffer(0L, mutable.Map[String, Long]())

    override def reduce(buff: CityBuffer, city: String): CityBuffer = {
      buff.total += 1L
      val map: mutable.Map[String, Long] = buff.cityMap
      val oldCnt: Long = map.getOrElse(city, 0L)
      map.update(city, oldCnt + 1L)
      buff.cityMap = map
      buff
    }

    override def merge(b1: CityBuffer, b2: CityBuffer): CityBuffer = {
      b1.total += b2.total
      b2.cityMap.foreach {
        case (city, cnt2) => {
          val oldCnt: Long = b1.cityMap.getOrElse(city, 0L)
          b1.cityMap.update(city, oldCnt + cnt2)
        }
      }
      b1
    }

    override def finish(buffer: CityBuffer): String = {
      val list = ListBuffer[String]()
      val total: Long = buffer.total
      val map: mutable.Map[String, Long] = buffer.cityMap
      val top2: List[(String, Long)] = map.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
      var t = 100L
      top2.foreach {
        case (city, cnt) => {
          val r = cnt * 100 / total
          t = t - r
          list.append(s"${city} ${r}%")
        }
      }
      if (map.size > 2) {
        list.append(s"其他 ${t}%")
      }

      list.mkString(",")
    }

    override def bufferEncoder: Encoder[CityBuffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
