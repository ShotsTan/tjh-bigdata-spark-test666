package com.tjh.bigdata.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions.{column, count, udf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @ClassName: SparkSQL13_DF_Req_Demo
 * @Description: TODO SparkSQL案例实现 -- 各区域热门商品Top3
 * @Author: Tanjh
 * @Date: 2023/01/12 9:45
 * @Company: Copyright©
 * */
object SparkSQL13_DF_Req_Demo {
  def main(args: Array[String]): Unit = {
    new SparkContext()
    val spark = new SparkSession.Builder().master("local[*]").appName("SparkSQL_Demo").enableHiveSupport().getOrCreate()
    spark.sql("use spark_01;")

    val len_UDF = udf((name: String) => name.length)
    val sc = spark.sparkContext

    //    df.select(column("city_name"),len_UDF(column("city_name")).as("len")).show()
    val df1: DataFrame = spark.sql(
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
        |""".stripMargin)

    df1.createTempView("t1")

    spark.udf.register("cityRemack", functions.udaf(new CityOrderUDF()))

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
        |       rank() over (partition by area order by cnt desc) as `rank`
        |        from t2
        |""".stripMargin).createTempView("t3")
    spark.sql(
      """
        |select * from t3
        |         where rank <= 3
        |                order by area desc, `rank`
        |""".stripMargin).show(false)


    spark.stop()
  }

  case class CityBuffer(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityOrderUDF extends Aggregator[String, CityBuffer, String] {
    override def zero: CityBuffer = CityBuffer(0L, (mutable.Map[String, Long]()))

    override def reduce(buffer: CityBuffer, city: String): CityBuffer = {
      buffer.total += 1L
      buffer.cityMap.update(city, buffer.cityMap.getOrElse(city, 0L) + 1L)
      buffer
    }

    override def merge(buffer1: CityBuffer, buffer2: CityBuffer): CityBuffer = {
      buffer1.total += buffer2.total
      buffer2.cityMap.foreach {
        case (city, cnt) => {
          buffer1.cityMap.update(city, buffer2.cityMap.getOrElse(city, 0L) + cnt)
        }
      }
      buffer1
    }

    override def finish(buffMap: CityBuffer): String = {
      val top2: List[(String, Long)] = buffMap.cityMap.toList.sortBy(x => x._2).take(2)
      var t = 100L
      val list: ListBuffer[String] = ListBuffer[String]()
      top2.foreach {
        case (city, cnt) => {
          t = t - cnt * 100 / buffMap.total
          list.append(s"${city}: ${cnt * 100 / buffMap.total}")
        }
      }
      if (buffMap.cityMap.size > 2) {
        list.append(s"其他: ${t}%")
      }
      list.mkString(",")
    }

    override def bufferEncoder: Encoder[CityBuffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
