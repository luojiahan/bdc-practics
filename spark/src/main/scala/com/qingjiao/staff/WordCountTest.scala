package com.qingjiao.staff

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * spark-submit --master spark://hadoop000:7077 --class com.qingjiao.staff.WordCountTest /root/wordcount/sparkwordcount.jar
 */
object WordCountTest {
  def main(args: Array[String]): Unit = {

    val begTime = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      //.master("local[*]") // 集群模式需要注释掉
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val input = "/root/word.txt"
    val output = "/root/wordcount/output1"
    //val input = "/root/software/hadoop-2.7.7/README.txt"
    //val output = "/root/wordcount/output2"
    val lines = sc.textFile("file://" + input)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)
    //将(you ,2 ) (hello ,3) 反转成(2 , you) (3 , hello)
    val reverseWordCounts = wordCounts.map(r => (r._2 , r._1))
    //通过key排序 sortByKey 参数为false ：倒序(3 , hello) (2 , you)
    val sortedCountWords = reverseWordCounts.sortByKey(false)
    //在将反转之后排序好的rdd在反转成：(hello ,3) (you , 2)
    val result = sortedCountWords.map(m => (m._2 , m._1))

    FileUtils.deleteDirectory(new File(output))
    result.saveAsTextFile("file://" + output)

    //result.map(x => "(" + x._1 + "," + x._2 + ")")
    //  .repartition(1)
    //  .saveAsTextFile("/root/wordcount/output1")

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()
  }
}
