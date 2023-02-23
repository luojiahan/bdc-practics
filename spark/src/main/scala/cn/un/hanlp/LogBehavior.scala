package cn.un.hanlp

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
 * spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.Logbh /root/spark.jar
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

    val hdfs: FileSystem = FileSystem.get(
      new java.net.URI(hdfs_url), new org.apache.hadoop.conf.Configuration())

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
    val writer = new PrintWriter(new File("/root/retrievelog/output/behavior"))
    val tuples = resultRDD.collect()
    for (elem <- tuples) {
      writer.write("("+elem._1 + "," + elem._2 + ")\n")
    }

    //数据保存位置
    val data_output: String = hdfs_url + "/root/retrievelog/output/behavior"
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    //将结果保存到HDFS
    resultRDD.map(x => "("+x._1+","+x._2+")")
      .repartition(1)
      .saveAsTextFile(data_output)


    // 将结果输出到mysql数据库中
//    resultRDD.foreachPartition(saveAsMySQL)

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()

  }
  def saveAsMySQL(iter:Iterator[(String,Int)]):Unit={
    //创建mysql链接
    Class.forName("com.mysql.jdbc.Driver") //注册Oracle的驱动
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?CharacterEncoding=UTF-8&useSSL=false",
        "root",
        "199037wW")
      conn.setAutoCommit(false) //设置手动提交
      pstmt = conn.prepareStatement("insert into url_count (url, count) values(?, ?)")
      //循环遍历写入数据库
      iter.foreach(f=>{
        pstmt.setString(1, f._1)
        pstmt.setInt(2, f._2)
        pstmt.addBatch()
      })
      pstmt.executeBatch() // 执行批量处理
      conn.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      pstmt.close()
      conn.close()
    }

  }
}
