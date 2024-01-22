package test

/**
 * @ClassName: Scala_Collect_Func
 * @Description: TODO 衍生集合
 * @Author: Tanjh
 * @Date: 2023/01/09 11:16
 * @Company: Copyright©
 * */
object Scala_Collect_Func02 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val map = Map(("a", 1), ("b", 2), ("c", 3), ("d", 5))

    // TODO 1.集合常用函数
    //    （1）获取集合的头
    println(list.head)
    println(map.head)
    println(list.headOption) //可获取空集合
    println("**" * 15)

    //    （2）获取集合的尾（不是头的就是尾）

    println(list.tail)
    println("**" * 15)

    //    （3）集合最后一个数据
    println(list.last)
    println(map.last)
    println("**" * 15)

    //    （4）集合初始数据（不包含最后一个）
    println(list.init)
    println(map.init)
    println("**" * 15)

    //    （5）反转
    println(list.reverse)
    println("**" * 15)

    //    （6）取前（后）n 个元素
    println(list.take(3))
    println(list.takeRight(3))
    println("**" * 15)

    //    （7）去掉前（后）n 个元素
    println(list.drop(3))
    println(list.dropWhile(i => i % 2 != 0))
    println("**" * 15)

    //    （8）并集
    println(list.union(list1))
    println("**" * 15)

    //    （9）交集
    println("**" * 15)

    //    （10）差集
    println("**" * 15)

    //    （11）拉链
    println("**" * 15)

    //    （12）滑窗

  }

}
