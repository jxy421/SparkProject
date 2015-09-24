package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author hdoop
 */
object RddCreate {
    def main(args: Array[String]): Unit = {
        val sc = getSparkContext();
		    //1.从集合创建RDD
        val rdd1 = sc.parallelize(1 to 9, 3)
        println(rdd1)
        //2. 外部存储创建
        val rdd2 = sc.parallelize("file:///F:/Project/SparkProject/WorkSpace/WordCount/file.txt", 3)
        println(rdd2)
    
    }
    def getSparkContext():SparkContext ={
    	val conf = new SparkConf();
    	conf.setAppName("Test")
    	conf.setMaster("local")
    	val sc = new SparkContext(conf)
      return sc;
    }
}