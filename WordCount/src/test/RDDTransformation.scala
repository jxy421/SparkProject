package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

/**
 * @author hdoop
 */
object RDDTransformation {
  def main(args: Array[String]): Unit = {
     //从集合创建RDD
     val sc = getSparkContext()
     val rdd1 = sc.parallelize(1 to 9, 3)
     /**
      * 2. mapPartitions
      *    mapPartitions(func)
      *    2.1 fnuc即为输入函数，它处理每个分区里面的内容。
      *    2.2 每个分区中的内容将以Iterator[T]传递给输入函数func
      *    2.3 func的输出结果是Iterator[U]
      *    2.4 最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
      */
     val rdd2 = rdd1.mapPartitions(func1)
     val ary = rdd2.collect()
     outAryII(ary)
     /**
      * 3 mapValues
      *   3.1 mapValues:就是输入函数应用于RDD中Kev-Value的Value
      *   3.2 原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素
      *   3.3 因此，该函数只适用于元素为KV对的RDD。
      */
     val rdd3 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
     val rdd4 = rdd3.map(x => (x.length, x))
     val ary2 = rdd4.mapValues("x" + _ + "x").collect
     outAryIS(ary2)
     /**
      * 4. mapWith
      *    4.1 mapWith是map的另外一个变种，map只需要一个输入函数
      *    4.2 mapWith有两个输入函数
      *    4.2.1 第一个函数constructA是把RDD的partition index（index从0开始）
      *          作为输入输出为新类型A
      *    4.2.2 第二个函数f是把二元组(T, A)作为输入,输出类型为U
      *          其中T为原RDD中的元素，A为第一个函数的输出
      *    举例：把partition index 乘以10，然后加上2作为新的RDD的元素。
      */
     val rdd5 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3) 
     //x.mapWith(a => a * 10)((a, b) => (a,b)).collect 
     //(1,0), (2,0), (3,0), (4,10), (5,10), (6,10), (7,20), (8,20), (9,20), (10,20)
     val ary3 = rdd5.mapWith(a => a * 10)((a, b) => (b + 2)).collect// Array(2, 2, 2, 12, 12, 12, 22, 22, 22, 22)
     outAryI(ary3)
     /**
      * 5. flatMap
      *    5.1 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素
      *    5.2 而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。 
      */
     val rdd6 = sc.parallelize(1 to 4, 2)
     val rdd7 = rdd6.flatMap(x => 1 to x)
     val ary4 = rdd7.collect()
     outAryI(ary4)
  }
  def func1[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[(T, T)]() 
    var pre = iter.next 
    while (iter.hasNext) {
        val cur = iter.next; 
        res .::= (pre, cur)//.::表示一个集合连接一个元素 
        pre = cur;
    } 
    res.iterator
  }
  def outAryI(ary:Array[Int]){
    for(ele <- ary){
        print("["+ele+"]")
      }
  }
  def outAryII(ary:Array[(Int,Int)]){
      for(ele <- ary){
        println("["+ele._1+","+ele._2+"]")
      }
  }
  def outAryIS(ary:Array[(Int,String)]){
      for(ele <- ary){
        println("["+ele._1+","+ele._2+"]")
      }
  }
  def getSparkContext():SparkContext ={
      val conf = new SparkConf();
      conf.setAppName("Test")
      conf.setMaster("local")
      val sc = new SparkContext(conf)
      return sc;
   }
}