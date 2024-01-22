package test

/**
 * @ClassName: Scala_Collect_Func
 * @Description: TODO 衍生集合
 * @Author: Tanjh
 * @Date: 2023/01/09 11:16
 * @Company: Copyright©
 * */
object Scala_Collect_Func03 {
  def main(args: Array[String]): Unit = {
    val list = List(13, 3, 55, 4, 51, 6, 67, 8, 19)
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val map = Map(("a", 1), ("b", 2), ("c", 3), ("d", 5))
    val tuples = List(("hello", 10), ("world", 2), ("scala", 9),("haha", 4),("hello", 1))

    // TODO 1.高阶函数
    //    （1）（5）排序
    println(list.sorted)
    println(list.sortBy(i => i))
    println(list.sortWith((x, y) => x > y))
    println(tuples.sortBy(x => x._2)(Ordering.Int.reverse))
    println(tuples.sortWith((x, y) => x._2 > y._2))
    println(tuples.sorted)

  }

}
