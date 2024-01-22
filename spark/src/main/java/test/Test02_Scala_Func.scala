package test

/**
 * @ClassName: Test02_Scala_Func
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/06 10:00
 * @Company: Copyright©
 * */
object Test02_Scala_Func {
  def main(args: Array[String]): Unit = {

    val function1 = (age: Int) => {
      println(age)
      age + ""
    }

    /*
    * TODO Java中集合:
    *  数组 : int [] arr=new int[5];
    *         double[] arr2 = {1.1, 2.2, 3.3, 60.6, 100}
    *         String[] arr3 = {"北京","jack","milan"};
    *  1.Collection: 单值类型
    *     1.1 List: 有序、可重复，支持索引; ArrayList、LinkedList、Vector
    *           List list = new ArrayList();
    *           list.add("jack");
    *           list.add("tom");
    *     1.2 Set: 无序、不重复、无索引; HashSet、LinkedHashSet
    *           Set<String> set = new HashSet<>();
    *           set.add("john");
    *           set.add("lucy");
    *  2.Map: K-V类型
    *     2.1 HashMap:Key不允许重复，Value可以重复
    *           HashMap map = new HashMap();
    *           map.put("java", 10);//ok
    *           map.put("php", 10);//ok
    *     2.2 HashTable
    *     2.3 Properties :通常作配置
    *
    * TODO Scala中集合 ，分为可变和不可变
    *  1.Seq :
    *     1.1 Array :数组                 (ArrayBuffer)
    *         val array1: Array[Int] = Array(1, 2, 3, 4)
    *     1.2 List : 数据有顺序，可重复      (ListBuffer)
    *         val list: List[Any] = List(1,1,1, 1.0, "hello", 'c')
    *         val list3 = List(1, 2, 3, 4)
    *  2.Set :无序不可重复                  (mutable.Set)
    *     val set: Set[Int] = Set(4, 3, 2, 1)
    *     val set1 = Set(1, 2, 3, 4, 2, 8, 4, 3, 7)
    *  3.Map :散列表，它存储的内容也是键值对（key-value）
    *     val map: Map[String, Int] = Map("hello" -> 1, "world" -> 2)
    *     val map1 = Map(("hello", 1), ("world", 2))
    *  4.Tuple : 常用二元组，元组中最大只能有 22 个元素
    *     val tuple: (Int, String, Boolean) = (40,"bobo",true)
    *
    * TODO Python
    *  1.列表  list = [1,2,3,4]
    *  2.元组  tuple = (1, 2, 3, 4)
    *  3.字典  dict = {'a': 1, 'b': 2, 'b': '3'}
    *  4.集合
    *     4.1 set可变集合: 无序不重复元素集 s = {'P', 'y', 't', 'h', 'o', 'n'}
    *     4.2 set不可变集合 :frozenset()
    *
    * */

    println()
  }

}
