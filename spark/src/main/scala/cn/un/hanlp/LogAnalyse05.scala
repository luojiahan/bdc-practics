package cn.un.hanlp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

/**
 * 需求：最优Rank频率，结果写入本地/root/retrievelog/output/rank/part-00000,格式见步骤说明。
 *
 * 对于用户搜索请求，某URL在返回结果中的排名为1, 且用户点击的顺序号为1，这说明此URL是Rank最优；
 * 计算用户访问网站URL在返回结果中排名为1，且用户点击顺序号为1的数据所占总数据的比率；
 * 过滤用户点击顺序号为1的数据，过滤返回结果中排名为1的数据，求出URL最优Rank数；
 * 最优Rank频率=URL最优Rank次数 / 条目总数，结果百分号前面保留两位小数，格式参考：11.11%；
 * 文件保存路径为：/root/retrievelog/output/rank/part-00000。
 */
object LogAnalyse05 {

  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf().setMaster("local[*]").setAppName("LogAnalyse01")

    val sc : SparkContext = new SparkContext(sparkConf)

    //加一个过滤
    val fileRDD: RDD[String] = sc.textFile("D:\\ideaIU-2021.1\\workspace\\bdc-practics\\data\\SogouQ.sample")
      .filter(_.split("\t").size==5).filter(_.split("\t")(3).split(" ").size==2)

    //获取排名
    val rankRDD: RDD[String] = fileRDD.map(_.split("\t")(3))

    //过滤最优rank
    val filterRDD: RDD[String] = rankRDD.filter(_.split(" ")(0)=="1").filter(_.split(" ")(1)=="1")

    //    filterRDD.foreach(println)

    //对两个rdd分别求和
    val sum = fileRDD.count().asInstanceOf[Double]

    val rank= filterRDD.count().asInstanceOf[Double]

    val result=rank/sum*100

    val str: String = result.formatted("%.2f")
    println(str)

    val writer = new PrintWriter(new File("F:\\result\\result05.txt"))

    writer.write(str+"%")

    sc.stop()
  }

}
