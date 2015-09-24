package word

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/**
 * @author hdoop
 */
object WordCount {
  def main(args: Array[String]) {
    val threshold=0
    val conf = new SparkConf();
    conf.setAppName("Spark Count")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    // split each document into words
    val tokenized = sc.textFile("file:///F:/Project/SparkProject/WorkSpace/WordCount/file.txt").flatMap(_.split(" "))
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)
    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    System.out.println(wordCounts.collect().mkString(", "))
  }
}