package cn.un.log

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Locale

object KPIIP {
  private val hdfs_url = "hdfs://hadoop000:9000"
  // 设置 hadoop用户名
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop000:9000")

    val hdfs: FileSystem = FileSystem.get(
      new java.net.URI(hdfs_url), new org.apache.hadoop.conf.Configuration())

    //1读取文件
    val file_path: String = hdfs_url + "/input/journal.log"
    val rawRDD: RDD[String] = sc.textFile(file_path)

    //2对日志进行过滤
    //58.248.178.212 - - [18/Sep/2013:06:51:40 +0000] "GET /wp-includes/js/jquery/jquery-migrate.min.js?ver=1.2.1 HTTP/1.1" 200 7200 "http://blog.fens.me/nodejs-grunt-intro/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MDDR; InfoPath.2; .NET4.0C)"

    val logRDD: RDD[String] = rawRDD
      // 过滤空行及数据量错误的行
      .filter(log => log != null && log.trim.split("\\s+").length > 11)
      //status大于400，HTTP错误
      .filter(log => log.trim.split("\\s+")(8).toInt < 400)

    println("==========================需求2==========================")
    //需求2：页面独立IP的访问量统计（IP）
    val ipRDD: RDD[String] = logRDD.map(_.split("\\s+")(0))
    val result2: RDD[(String, Int)] = ipRDD.map((_, 1)).reduceByKey(_ + _)


    val data_output2: String = hdfs_url + "/internetlogs/ip"
    if (hdfs.exists(new Path(data_output2)))
      hdfs.delete(new Path(data_output2), true)

    result2.map(x => x._1 + "\t" + x._2)
      .repartition(1)
      .saveAsTextFile(data_output2)


  }
}
