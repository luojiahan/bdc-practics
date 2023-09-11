package com.qingjiao.spark.streaming.traning.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, LongType}

object Analysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Analysis").master("local[*]").getOrCreate()

    import spark.implicits._
    // 从kafka指定的topic中读取数据
    val df = UserUtils.getKafkaConsumer(spark, "iot")

    // 解析读取的数据: value => jsonString => 获取对应字段的值
    val valueDF = df.selectExpr("cast(value as String)").as[String].select(
      get_json_object($"value", "$.device").as("device_id"),
      get_json_object($"value", "$.deviceType").as("device_type"),
      get_json_object($"value", "$.signal").cast(DoubleType).as("signal"),
      get_json_object($"value", "$.time").cast(LongType).as("time")
    )

    // 分析
    valueDF.createOrReplaceTempView("iot_view")
    val sql=
      """
        |select device_type,count(*) as device_count,avg(signal) as avg_signal from iot_view
        |where signal > 30 group by device_type
        |""".stripMargin

    val result = spark.sql(sql)

    result.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate",false)
      .start()
      .awaitTermination()

    spark.stop()
  }

}
