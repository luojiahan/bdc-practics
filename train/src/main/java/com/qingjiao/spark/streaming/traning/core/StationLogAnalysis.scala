package com.qingjiao.spark.streaming.traning.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object StationLogAnalysis {
  // 将日期时间转换为时间戳
  def parseDateTime(dateTime:String): Long = {
    // 定义日期时间格式
    val sourceDateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    // 将字符串类型的日期时间数据转换为LocalDateTime
    val localDateTime = LocalDateTime.parse(dateTime, sourceDateTimeFormatter)

    // 将LocalDateTime转换为时间戳
    val ts = localDateTime.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    ts
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("StationLogAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 要打开的文件
    // 1. 用户信息
    val userFile="/Users/kim/IdeaProjects/SparkTreaning0822/src/main/resources/station/user.txt"
    // 2. 基站信息
    val infoFile="/Users/kim/IdeaProjects/SparkTreaning0822/src/main/resources/station/info.txt"

    // 分别打开2个文件
    // 13554756349,20211202082000,16030401EAFB68F1E3CDF819735E1C66,1
    val userRDD = sc.textFile(userFile)
    // 9F36407EAD0629FC166F14DDE7970F68,116.304864,40.050645,6
    val infoRDD = sc.textFile(infoFile)

    val phoneStationTime = userRDD.mapPartitions(it => {
      it.map(line => {
        val contents = line.split(",")
        // ((phone,stationID),ts) ==> ts进入基站时间为负
        val ts = if ("1" == contents(3)) -parseDateTime(contents(1)) else parseDateTime(contents(1))
        ((contents(0), contents(2)), ts)
      })
    }).reduceByKey(_ + _).sortBy(_._2, false)

    val stationLocation = infoRDD.mapPartitions(it => {
      it.map(line => {
        val contents = line.split(",")
        // (stationID,(lat,lon))
        (contents(0), (contents(1), contents(2)))
      })
    })

    val stationPhoneTime = phoneStationTime.map(item => {
      // (stationId,(phone,ts))
      (item._1._2, (item._1._1, item._2))
    })

    // join:(CC0710CC94ECC657A8561DE549D940E0,((13236723613,1140000),(116.303955,40.041935)))
    val joinResult = stationPhoneTime.join(stationLocation).map(item=>{
      // (phone,stationID,ts,(lat,lon))
      (item._2._1._1,item._1,item._2._1._2,item._2._2)
    }).groupBy(_._1).mapValues(item=>{
      item.toList.sortBy(_._3).reverse.take(2)
    })

    joinResult.collect().foreach(println)
    sc.stop()
  }

}
