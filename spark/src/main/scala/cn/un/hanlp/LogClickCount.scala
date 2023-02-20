package cn.un.hanlp

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.mutable
/**
 * 需求2：
 * 用户搜索点击统计，使用[HanLP](https://so.csdn.net/so/search?q=HanLP&spm=1001.2101.3001.7020)对所有查询词（字段3）进行分词，
 * 按照用户ID和查询词进行分组聚合统计出现次数,结果写入本地/root/retrievelog/output/click/part-00000,格式见步骤说明。
 *
 * 获取搜索词（字段3），进行[中文分词]；
 * 使用HanLP中文分词库进行[分词]，对应依赖包为hanlp-portable-1.8.2.jar（见左侧“竞赛信息“-“附件资料“）；
 * 统计用户搜索点击次数，按照（用户ID，查询词）分组统计次数，词频降序排序；
 * 文件保存路径为：/root/retrievelog/output/click/part-00000，结果无需分区；
 * 示例结果：((491312143310257,律师),1)表示用户491312143310257搜索关键词律师的词频计数为1。
 * cp hanlp/hanlp-1.8.2-release/hanlp-portable-1.8.2.jar software/spark-2.4.3-bin-hadoop2.7/jars/
 * spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogClickCount /root/spark.jar
 */
object LogClickCount {
  private val hdfs_url = "hdfs://hadoop000:9000"
  // 设置 hadoop用户名
  System.setProperty("HADOOP_USER_NAME", "root")

  case class SogouRecord(
                          queryTime: String,
                          userId: String,
                          queryWords: String,
                          resultRank: Int,
                          clickRank: Int,
                          clickUrl: String
                        )
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

    // 在scala中，\s是用来匹配任何空白字符，当\放在最前面，前面得再放个\，或者在scala中用"""\s+"""
    val SogouRecordRDD: RDD[SogouRecord] = fileRDD
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

    val clickCountRDD: RDD[(String, String)] = SogouRecordRDD.flatMap(record => {
      // 使用HanLP中文分词库进行标准分词
      val terms: util.List[Term] = StandardTokenizer.segment(record.queryWords.trim)
      // 将Java中集合对转换为Scala中集合对象
      import scala.collection.JavaConverters._
      val words: mutable.Buffer[String] = terms.asScala.map(_.word)
      val userId: String = record.userId
      words.map(word => (userId, word))
    })
    val clickCount: RDD[((String, String), Int)] = clickCountRDD
      .filter(t => !t._2.equals(".") && !t._2.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    //数据保存位置
    val data_output: String = hdfs_url + "/root/retrievelog/output/click"
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    //将结果保存到HDFS
    clickCount.map(x => "("+x._1+","+x._2+")")
      .repartition(1)
      .saveAsTextFile(data_output)

    val endTime = System.currentTimeMillis()
    println("用时：" + (endTime - begTime) / 1000 + "s")
    sc.stop()
  }

}
