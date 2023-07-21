package cn.un.hanlp

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 需求1：
 * 网站（URL）访问量统计，对URL进行过滤，统计日志数据中各个网站URL首页的访问次数，结果写入本地/root/retrievelog/output/url/part-00000,格式见步骤说明。
 * 对URL进行过滤，获取首页网站的访问量，只统计www开头的首页网站；
 * 过滤以www开头的所有URL，对URL进行/切分，获取首页网址，如www.tudou.com；
 * 首页网址搜索频率统计，按首页网址分组聚合，根据频率进行降序排列；
 * 文件保存路径为：/root/retrievelog/output/url/part-00000，结果无需分区；
 * 示例结果：(www.tudou.com,28714) 表示网站URLwww.tudou.com的访问次数为28714。
 * spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogBehavior /root/spark.jar
 */
object LogBehavior {

  private val hdfs_url = "hdfs://hadoop000:9000"
  // 设置 hadoop用户名
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val begTime = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
//      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    sc.hadoopConfiguration.set("fs.defaultFS", hdfs_url)

    val input_path: String = hdfs_url+ "/input/reduced.txt"

    val fileRDD: RDD[String] = sc.textFile(input_path)

    val filterRDD: RDD[String] = fileRDD
      .filter(log => log != null && log.trim.split("\\s+").length == 6)

    //获取排名
    val rankRDD: RDD[String] = fileRDD.map(_.split("\t")(3))

    val resultRDD: RDD[(String, Int)] = rankRDD
      .map(line => (line.split(" ")(0),1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    //将结果写入本地文件
    val out_path = "/root/retrievelog/output/behavior/"
    FileUtils.deleteDirectory(new File(out_path))
    resultRDD.map(x => "("+x._1+","+x._2+")")
      .repartition(1)
      .saveAsTextFile("file://" + out_path)

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()

  }
}
