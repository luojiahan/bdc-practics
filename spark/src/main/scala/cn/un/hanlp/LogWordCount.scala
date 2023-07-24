package cn.un.hanlp

import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import java.util
import scala.collection.mutable

/**
 * 需求3：
 * 查询关键词分析，使用[HanLP](https://so.csdn.net/so/search?q=HanLP&spm=1001.2101.3001.7020)对所有查询词（字段3）进行分词，
 * 按照分词进行分组聚合统计出现次数,结果写入本地/root/retrievelog/output/key/part-00000,格式见步骤说明。
 *
 * 获取搜索词（字段3），进行[中文分词]；
 * 使用HanLP中文分词库进行[分词]，对应依赖包为hanlp-portable-1.8.2.jar（见左侧“竞赛信息“-“附件资料“）；
 * 统计搜索词出现次数，分组统计次数，词频降序排序；
 * 文件保存路径为：/root/retrievelog/output/key/part-00000，结果无需分区；
 * 示例结果：(69239,物资)表示关键词物资的词频计数为69239。
 * spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogWordCount /root/jars/spark.jar
 */
object LogWordCount {

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

    // 过滤
    val filterRDD: RDD[String] = fileRDD
      .filter(log => log != null && log.trim.split("\\s+").length == 6)

    // 访问时间 用户ID [查询词] 该URL在返回结果中的排名 用户点击的顺序号 用户点击的URL
    val keywordRDD: RDD[String] = filterRDD.map(_.split("\\s+")(2))
      .map(_.replaceAll("\\[|\\]", ""))

    val wordsRDD: RDD[String] = keywordRDD.flatMap(record => {
      // 使用HanLP中文分词库进行标准分词
      val terms: util.List[Term] = StandardTokenizer.segment(record.trim)
      // 将Java中集合对转换为Scala中集合对象
      import scala.collection.JavaConverters._
      val words: mutable.Buffer[String] = terms.asScala.map(_.word)
      // val userId: String = record.userId
      // words.map(word => (userId, word))
      words
    })

//    println(s"Count = ${wordsRDD.count()}, Example = ${wordsRDD.take(5).mkString(",")}")
    //wordcount操作
    val resultRDD: RDD[(String, Int)] = wordsRDD
      .filter(t => !t.equals(".") && !t.equals("+"))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)

    //将结果写入本地文件
    val out_path = "/root/retrievelog/output/key/"
    FileUtils.deleteDirectory(new File(out_path))
    resultRDD.map(x => "("+x._1+","+x._2+")")
      .repartition(1)
      .saveAsTextFile("file://" + out_path)

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()
  }

}
