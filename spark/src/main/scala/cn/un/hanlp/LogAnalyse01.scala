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
 */
object LogAnalyse01 {

  private val hdfs_url = "hdfs://192.168.10.101:9000"
  // 设置 hadoop用户名
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop000:9000")

    val input_path: String = hdfs_url+ "/input/reduced.txt"
    val fileRDD: RDD[String] = sc.textFile(input_path)

    // 过滤错误格式的日志条目
    val filterRDD: RDD[String] = fileRDD
      .filter(log => log != null && log.trim.split("\\s+").length == 6)
    val urlRDD: RDD[String] = filterRDD.map(_.split("\\s+")(5))

    // 过滤以www开头的网址
    val wwwRDD: RDD[String] = urlRDD.filter(_.startsWith("www"))
    // 获取网址首页
    val oneUrlRDD: RDD[String] = wwwRDD.map(_.split("/")(0))
    // 计算结果
    val resultRDD: RDD[(String, Int)] = oneUrlRDD.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)

    // 测试用户的日志
    println(s"Count = ${resultRDD.count()}, Example = ${resultRDD.take(5).mkString(",")}")

    //将结果写入本地文件
//    val writer = new PrintWriter(new File("F:\\result\\result01.txt"))
//    val tuples = resultRDD.collect()
//    for (elem <- tuples) {
//      writer.write("("+elem._1 + "," + elem._2 + ")\n")
//    }

    //数据保存位置
    val data_output: String = hdfs_url + "/root/retrievelog/output/url"

    val hdfs: FileSystem = FileSystem.get(
      new java.net.URI(hdfs_url), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    //将结果保存到HDFS
    resultRDD.map(x => "("+x._1+","+x._2+")")
      .repartition(1)
      .saveAsTextFile(data_output)


    // 将结果输出到mysql数据库中
    // 这是在每个分区中，e就是一个分区中数据访问和操作的迭代器
    resultRDD.foreachPartition(iter => {
      // 在每个分区中建立写入到mysql的连接而不是最后结果汇总到dirver端再写入，
      // 这样可以充分利用分布式集群的优势，否则海量数据汇总到driver端再写入，耗时会非常非常久，不合适
      // 同样的，注意不要或者避免再driver端做asList或者toArray等操作，这样会把数据加载到内存，处理海量数据时，很可能造成内存溢出
      Class.forName("com.mysql.jdbc.Driver")

      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection("jdbc:mysql://hdp103:3306/test?CharacterEncoding=UTF-8", "root", "199037")

        // 这里使用preparedStatement，有几个好处。第一个是参数可调整，拼接方式。第二个是防止sql注入，第三个是会有编译缓存，执行起来更快
        preparedStatement = connection.prepareStatement("insert into url_count (url, count) values(?, ?)")

        // 遍历每个分区中的数据，写入到mysql中
        iter.foreach(e => {
          preparedStatement.setString(1, e._1)
          preparedStatement.setInt(2, e._2)

          // 这里实际企业生产中，遇到批量更新，可以使用preparedStatement.addBatch（）,然后使用条件来判断什么时候preparedStatement.executeBatch()
          // 在处理海量数据过程中，一般都不会一条一条插入，而是批量插入。一般是200到几百条做一次插入，具体看内存和插入数据量。太多可能会导致内存不足
          preparedStatement.executeLargeUpdate()
        })
      } catch {
        case e: Exception => {
          println("出现sql异常了，一般做事务回滚操作，rollback")
          println(e)
        }
      } finally {
        // 关流
        try {
          if (preparedStatement != null) {
            preparedStatement.close()
          }
          if (connection != null) {
            connection.close()
          }
        } catch {
          case e: Exception => {
            println("sql关流失败了")
          }
        }
      }
    })

    sc.stop()

  }
}
