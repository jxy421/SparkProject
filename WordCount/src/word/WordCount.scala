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
    val inpath="file:///F:/Project/SparkProject/SparkProject/WordCount/file.txt"
    val outpath="file:///F:/Project/SparkProject/SparkProject/WordCount/out"
    System.setProperty("hadoop.home.dir", "F:/Project/SparkProject/hadoop-2.6.0/hadoop-2.6.0")
    val sc = new SparkContext(conf)
    // split each document into words
    val tokenized = sc.textFile(inpath).flatMap(_.split(" "))
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(outpath)
//    filter out words with less than threshold occurrences
//    val filtered = wordCounts.filter(_._2 >= threshold)
//    // count characters
//    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
//    System.out.println(wordCounts.collect().mkString(", "))
  }
}