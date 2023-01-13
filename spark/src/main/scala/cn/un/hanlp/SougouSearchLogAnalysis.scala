package cn.un.hanlp

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.mutable

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1．搜索关键词统计，使用HanLP中文分词
 * 2．用户搜索次数统计
 * 3．搜索时间段统计*数据格式:访问时间\t用户ID\t[查询词]\t i该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 */
object SougouSearchLogAnalysis {

    val driver = "com.mysql.jdbc.Driver"
    val user = "root"
    val password = "199037"
    val url = "jdbc:mysql://hdp103:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

    private val hdfs_url = "hdfs://hadoop000:9000"
    // 设置 hadoop用户名
    System.setProperty("HADOOP_USER_NAME", "root")

    /**
     * 用户搜索点击网页记录Record
     *
     * @param queryTime  访问时间，格式为：HH:mm:ss
     * @param userId     用户ID
     * @param queryWords 查询词
     * @param resultRank 该URL在返回结果中的排名
     * @param clickRank  用户点击的顺序号
     * @param clickUrl   用户点击的URL
     */
    case class SogouRecord(
                            queryTime: String,
                            userId: String,
                            queryWords: String,
                            resultRank: Int,
                            clickRank: Int,
                            clickUrl: String
                          )

    def main(args: Array[String]): Unit = {

      val spark: SparkSession = SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
      val sc: SparkContext = spark.sparkContext
      sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
      sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop000:9000")

      // TODO: 1. 本地读取SogouQ用户查询日志数据。本地文件需要加file:///
      val input_path: String = hdfs_url+ "/input/SogouQ.sample"
      val rawLogsRDD: RDD[String] = sc.textFile(input_path)
      //println(s"Count = ${rawLogsRDD.count()}")

      // TODO: 2. 解析数据，封装到CaseClass样例类中
      // 在scala中，\s是用来匹配任何空白字符，当\放在最前面，前面得再放个\，或者在scala中用"""\s+"""
      val SogouRecordRDD: RDD[SogouRecord] = rawLogsRDD
        // 过滤不合法数据，如null，分割后长度不等于6
        .filter(log => log != null && log.trim.split("\\s+").length == 6)
        // 对每个分区中数据进行解析，封装到SogouRecord
        .mapPartitions(iter => {
          iter.map(log => {
            val arr: Array[String] = log.trim.split("\\s+")
            SogouRecord(
              arr(0),
              arr(1),
              arr(2).replaceAll("\\[|\\]", ""),
              arr(3).toInt,
              arr(4).toInt,
              arr(5)
            )
          })
        })
      // println(s"Count = ${SogouRecordRDD.count()},\nFirst = ${SogouRecordRDD.first()}")

      // 数据使用多次，进行缓存操作，使用count触发
      SogouRecordRDD.persist(StorageLevel.MEMORY_AND_DISK)

      //TODO 3.1 =================== 3.1 搜索关键词统计 ===================
      // a. 获取搜索词，进行中文分词
      val wordsRDD: RDD[String] = SogouRecordRDD.mapPartitions(iter => {
        iter.flatMap(record => {
          // 使用HanLP中文分词库进行分词
          val terms: util.List[Term] = HanLP.segment(record.queryWords.trim)
          // 将Java中集合对转换为Scala中集合对象
          import scala.collection.JavaConverters._
          terms.asScala.map(_.word)
        })
      })
      // println(s"Count = ${wordsRDD.count()}, Example = ${wordsRDD.take(5).mkString(",")}")

      // b. 统计搜索词出现次数，获取次数最多Top10
      val top10SearchWords: Array[(Int, String)] = wordsRDD
        .filter(word => !word.equals(".") && !word.equals("+"))
        .map((_, 1)) // 每个单词出现一次
        .reduceByKey(_ + _) // 分组统计次数
        .map(_.swap)
        .sortByKey(ascending = false) // 词频降序排序
        .take(10) // 获取前10个搜索词

      //TODO 3.2 =================== 3.2 用户搜索点击次数统计 ===================
      /*
          每个用户在搜索引擎输入关键词以后，统计点击网页数目，反应搜索引擎准确度
       */
      val clickCountRDD: RDD[(String, String)] = SogouRecordRDD.flatMap(record => {
        // 使用HanLP中文分词库进行分词
        val terms: util.List[Term] = HanLP.segment(record.queryWords.trim)
        // 将Java中集合对转换为Scala中集合对象
        import scala.collection.JavaConverters._
        val words: mutable.Buffer[String] = terms.asScala.map(_.word)
        val userId: String = record.userId
        words.map(word => (userId, word))
      })
      val top10clickCount: Array[((String, String), Int)] = clickCountRDD
        .filter(t => !t._2.equals(".") && !t._2.equals("+"))
        .map((_, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, false)
        .take(10)

      //TODO 3.3 =================== 3.3 搜索时间段统计 ===================
      /*
          从搜索时间字段获取小时，统计每个小时搜索次数
       */
      val hourSearchCount: RDD[(String, Int)] = SogouRecordRDD.map(record => {
        // 提取小时和分钟
        // 03:12:50
        val timeStr: String = record.queryTime
        val hourAndMitunesStr: String = timeStr.substring(0, 5)
        (hourAndMitunesStr, 1)
      })
        .reduceByKey(_ + _) // 分组统计次数
        .sortBy(_._2, ascending = false)
      //      .take(10)

      //TODO 4.输出结果
      println("=================== 3.1 搜索关键词统计 ===================")
      top10SearchWords.foreach(println)
      println("=================== 3.2 用户搜索点击次数统计 ===================")
      top10clickCount.foreach(println)
      println("=================== 3.3 搜索时间段统计 ===================")
      hourSearchCount.foreach(println)
      // 写入hdfs
      //hourSearchCount.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile("/result/3/")
      hourSearchCount.map(x => x._1 + "\t" + x._2).repartition(1).saveAsTextFile("/result/3/")
      //写入mysql操作
      hourSearchCount.foreachPartition {
        data => {
          //注册驱动
          Class.forName(driver)
          //获取连接
          val connection: Connection = DriverManager.getConnection(url, user, password)
          //声明数据库操作sql语句
          var sql = "insert into hour_count values(?,?) "
          //创建数据库操作对象
          val ps: PreparedStatement = connection.prepareStatement(sql)
          data.foreach {
            //数据匹配
            case (time, count) => {
              //设置参数
              ps.setString(1, time)
              ps.setInt(2, count)
              //执行sql
              ps.executeUpdate();
            }
          }
          //关闭连接和数据库操作对象
          ps.close()
          connection.close()
        }
      }

      //TODO 5. 释放资源
      sc.stop()
    }

}
