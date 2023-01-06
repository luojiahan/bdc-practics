package cn.un.hanlp

import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import java.util

/**
 * 需求：
 * 查询关键词分析，使用[HanLP](https://so.csdn.net/so/search?q=HanLP&spm=1001.2101.3001.7020)对所有查询词（字段3）进行分词，
 * 按照分词进行分组聚合统计出现次数,结果写入本地/root/retrievelog/output/key/part-00000,格式见步骤说明。
 *
 * 获取搜索词（字段3），进行[中文分词]；
 * 使用HanLP中文分词库进行[分词]，对应依赖包为hanlp-portable-1.8.2.jar（见左侧“竞赛信息“-“附件资料“）；
 * 统计搜索词出现次数，分组统计次数，词频降序排序；
 * 文件保存路径为：/root/retrievelog/output/key/part-00000，结果无需分区；
 * 示例结果：(69239,物资)表示关键词物资的词频计数为69239。
 */
object LogAnalyse03 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val fileRDD: RDD[String] = sc.textFile("D:\\ideaIU-2021.1\\workspace\\bdc-practics\\data\\SogouQ.sample")

    // 过滤
    val filterRDD: RDD[String] = fileRDD
      .filter(log => log != null && log.trim.split("\\s+").length == 6)
    val keywordRDD: RDD[String] = filterRDD.map(_.split("\\s+")(2))
      .map(_.replaceAll("\\[|\\]", ""))
    val hanlpRDD: RDD[util.List[Term]] = keywordRDD.map(StandardTokenizer.segment(_))

    val StringRDD: RDD[String] = hanlpRDD.map(
      str=>{
        var re=str.get(0).word
        for(i <- 1 to str.size()-1){
          re=re+"|=|"+str.get(i).word
        }
        re
      }
    )
    val value: RDD[String] = StringRDD.flatMap(_.split("\\|=\\|"))

    println(s"Count = ${value.count()}, Example = ${value.take(5).mkString(",")}")
    //wordcount操作
    val resultRDD: RDD[(String, Int)] = value.map((_,1)).reduceByKey(_+_).sortBy(_._2,false)

    val writer = new PrintWriter(new File("F:\\result\\result03.txt"))
    val tuples = resultRDD.collect()
    for (elem <- tuples) {
      writer.write("("+elem._2 + "," + elem._1 + ")\n")
    }



    sc.stop()


  }

}
