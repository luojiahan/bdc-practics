package cn.un.hanlp

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求5：最优Rank频率，结果写入本地/root/retrievelog/output/rank/part-00000,格式见步骤说明。
 *
 * 对于用户搜索请求，某URL在返回结果中的排名为1, 且用户点击的顺序号为1，这说明此URL是Rank最优；
 * 计算用户访问网站URL在返回结果中排名为1，且用户点击顺序号为1的数据所占总数据的比率；
 * 过滤用户点击顺序号为1的数据，过滤返回结果中排名为1的数据，求出URL最优Rank数；
 * 最优Rank频率=URL最优Rank次数 / 条目总数，结果百分号前面保留两位小数，格式参考：11.11%；
 * 文件保存路径为：/root/retrievelog/output/rank/part-00000。
 */
object LogRank {

  private val hdfs_url = "hdfs://hadoop000:9000"
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
    sc.hadoopConfiguration.set("fs.defaultFS", hdfs_url)

    val hdfs: FileSystem = FileSystem.get(
      new java.net.URI(hdfs_url), new org.apache.hadoop.conf.Configuration())

    //加一个过滤
    val input_path: String = hdfs_url+ "/input/reduced.txt"
    val fileRDD: RDD[String] = sc.textFile(input_path)
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

    val resultStr: String = result.formatted("%.2f")
    println(resultStr)
    val resultRDD: RDD[String] = sc.parallelize(List(resultStr))

    //数据保存位置
    val data_output: String = hdfs_url + "/output/result5"
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    //将结果保存到HDFS
    resultRDD.map(x => x + "%")
      .repartition(1)
      .saveAsTextFile(data_output)

    sc.stop()
  }

}
