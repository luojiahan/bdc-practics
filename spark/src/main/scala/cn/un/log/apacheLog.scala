package cn.un.log

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.text.SimpleDateFormat
import java.util.Locale

object apacheLog {
  private val hdfs_url = "hdfs://hadoop000:9000"
  // 设置 hadoop用户名
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val begTime = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    sc.hadoopConfiguration.set("fs.defaultFS", hdfs_url)

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

    // 数据使用多次，进行缓存操作，使用count触发
    logRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("==========================需求1==========================")
    // 需求1：页面访问量统计（PV）
    val pvUrl = List("/about",
      "/black-ip-list/",
      "/cassandra-clustor/",
      "/finance-rhive-repurchase/",
      "/hadoop-family-roadmap/",
      "/hadoop-hive-intro/",
      "/hadoop-zookeeper-intro/",
      "/hadoop-mahout-roadmap/")
    val pvRDD: RDD[String] = logRDD.map(_.split("\\s+")(6))
      .filter(request => pvUrl.contains(request))

    val result1: RDD[(String, Int)] = pvRDD.map((_, 1)).reduceByKey(_ + _)

    //数据保存位置
    //val data_output1: String = hdfs_url + "/internetlogs/pv"
    //if (hdfs.exists(new Path(data_output1)))
    //  hdfs.delete(new Path(data_output1), true)
    val data_output1: String = "file://root/internetlogs/pv/"
    FileUtils.deleteDirectory(new File(data_output1))

    //将结果保存到HDFS
    result1.map(x => x._1 + "\t" + x._2)
      .repartition(1)
      .saveAsTextFile(data_output1)

    println("==========================需求2==========================")
    //需求2：页面独立IP的访问量统计（IP）
    val ipRDD: RDD[String] = logRDD.map(_.split("\\s+")(0))
    val result2: RDD[(String, Int)] = ipRDD.map((_, 1)).reduceByKey(_ + _)

    //val data_output2: String = hdfs_url + "/internetlogs/ip"
    //if (hdfs.exists(new Path(data_output2)))
    //  hdfs.delete(new Path(data_output2), true)
    val data_output2: String = "file:///root/internetlogs/ip/"
    FileUtils.deleteDirectory(new File(data_output2))

    result2.map(x => x._1 + "\t" + x._2)
      .repartition(1)
      .saveAsTextFile(data_output2)

    println("==========================需求3==========================")
    //需求3：每小时访问网站的次数统计(time)
    val timeRDD: RDD[String] = logRDD
      .map(line => {
        val dateStr: String = line.split("\\s+")(3).substring(1)
        val df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US)
        val df2 = new SimpleDateFormat("yyyyMMddHH")
        val timeStr: String = df2.format(df.parse(dateStr))
        timeStr
      })
    val result3: RDD[(String, Int)] = timeRDD.map((_, 1)).reduceByKey(_ + _)

    // 测试用户的日志
//    println(s"Count = ${timeRDD.count()}, Example = ${timeRDD.take(3).mkString(",")}")

    //val data_output3: String = hdfs_url + "/internetlogs/time"
    //if (hdfs.exists(new Path(data_output3)))
    //  hdfs.delete(new Path(data_output3), true)
    val data_output3: String = "file:///root/internetlogs/time/"
    FileUtils.deleteDirectory(new File(data_output3))

    result3.map(x => x._1 + "\t" + x._2)
      .repartition(1)
      .saveAsTextFile(data_output3)

    println("==========================需求4==========================")
    //需求4：访问网站的浏览器标识统计(browser)
    val browserRDD: RDD[String] = logRDD.map(line => {
      val fields: Array[String] = line.split("\\s+")
      var http_agent = ""
      if (fields.length > 12) {
        http_agent = fields(11) + " " + fields(12)
      } else {
        http_agent = fields(11)
      }
      http_agent
    })
    val result4: RDD[(String, Int)] = browserRDD.map((_, 1)).reduceByKey(_ + _)

    //val data_output4: String = hdfs_url + "/internetlogs/browser"
    //if (hdfs.exists(new Path(data_output4)))
    //  hdfs.delete(new Path(data_output4), true)
    val data_output4: String = "file:///root/internetlogs/browser/"
    FileUtils.deleteDirectory(new File(data_output4))

    result4.map(x => x._1 + "\t" + x._2)
      .repartition(1)
      .saveAsTextFile(data_output4)

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()
  }
}
