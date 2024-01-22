package test

/**
 * @ClassName: Scala_Collect_Func
 * @Description: TODO 高级函数
 * @Author: Tanjh
 * @Date: 2023/01/09 11:16
 * @Company: Copyright©
 * */
object Scala_Collect_Func04 {
  def main(args: Array[String]): Unit = {
    val list = List(13, 3, 55, 4, 51, 6, 67, 8, 19)
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val map = Map(("a", 1), ("b", 2), ("c", 3), ("d", 5))
    val tuples = List(("hello", 10), ("world", 2), ("scala", 9), ("haha", 4), ("hello", 1))
    val list2: List[String] = List("hello hadoop", "hei spark", "hello fink")

    // TODO 1.高阶函数
    //    （1）过滤 遍历一个集合并从中获取满足指定条件的元素组成一个新的集合。
    println("****** filter ********")
    println(list.filter(x => x % 2 == 0))
    println(tuples.filter {
      case (word, cnt) => {
        if (word == "hello") false
        else true
      }
    })
    println("****** map ********")
    //    （2）转化/映射（map）
    println(map.map(x => (x, x._2, 1)))
    println(list.map(x => x + 100))

    println("****** flatMap ********")
    //    （3）扁平化
    println(list2.map(x => x.split(" ")).flatten)

    //    （4）扁平化+映射 注：flatMap 相当于先进行 map 操作，在进行flatten 操作。集合中的每个元素的子元素映射到某个函数并返回新集合。
    println(list2.flatMap(x => x.split(" ")))

    //    （5）分组（groupBy）按照指定的规则对集合的元素进行分组。
    println(tuples.groupBy(x => x._2))

    //    （6）简化（归约）
    println(list.reduce((x, y) => x + y))
    println(map.reduce((x, y) => (x._1, x._2 + y._2)))

    //    （7）折叠
    println(list.fold(0)(_ + _))


  }

}
