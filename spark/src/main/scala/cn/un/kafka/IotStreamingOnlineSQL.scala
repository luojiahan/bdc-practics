package cn.un.kafka

import cn.un.kafka.util.{HbaseSink, JDBCSink}
import org.apache.commons.lang3.StringUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 对物联网设备状态信号数据，实时统计分析，基于SQL编程
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object IotStreamingOnlineSQL {

  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession会话实例对象，设置属性信息
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 2. 从Kafka读取数据，底层采用New Consumer API
    val iotStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hdp101:9092,hdp102:9092,hdp103:9092")
      .option("subscribe", "iotTopic")
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", "100000")
      .load()

    // 3. 对获取数据进行解析，封装到DeviceData中
    val etlStreamDF: Dataset[IotInfo] = iotStreamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 将数据转换Dataset
      .as[String] // 内部字段名为value
      // 过滤数据
      .filter(StringUtils.isNotBlank(_))
      // 解析JSON数据：{"device":"device_65","deviceType":"db","signal":12.0,"time":1589718910796}
      .map(record => {
        val jsonObject: JSONObject = JSON.parseObject(record)
        IotInfo(
          jsonObject.getString("device"),
          jsonObject.getString("deviceType"),
          jsonObject.getString("signal"),
          jsonObject.getString("time")
        )
      })

    // 4. 依据业务，分析处理
    // TODO: signal > 30 所有数据，按照设备类型 分组，统计数量、平均信号强度
    // 4.1 注册DataFrame为临时视图
    etlStreamDF.createOrReplaceTempView("t_iots")
    // 4.2 编写SQL执行查询
    val resultStreamDF: DataFrame = spark.sql(
      """
        |SELECT
        |  device_id,
        |  device_type,
        |  COUNT(device_type) AS count_device,
        |  ROUND(AVG(signal), 2) AS avg_signal
        |FROM t_iots
        |WHERE signal > 30 GROUP BY device_id, device_type
        |""".stripMargin)
//      .as[IotStaticInfo]

    //使用
    val url="jdbc:mysql://hdp103:3306/test?characterEncoding=utf8&useSSL=false"
    val user ="root"
    val pwd = "199037"
    val writer = new JDBCSink(url,user, pwd)

    resultStreamDF.writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(ProcessingTime("25 seconds"))
      .start()
      .awaitTermination()


  }
  //接收各个字段:{"device":"device_65","deviceType":"db","signal":12.0,"time":1589718910796}
  case class IotInfo(
                      device_id: String,
                      device_type: String,
                      signal: String,
                      time: String
                    )
  case class IotStaticInfo(
                            device_id: String,
                            device_type: String,
                            count_device: String,
                            avg_signal: String
                    )
}
