package com.qingjiao.spark.streaming.traning.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, count, round}

import java.util.Properties

object MovieRatingAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("MovieRatingAnalysis").master("local[*]").getOrCreate()

    import spark.implicits._

    // 要打开的文件
    val file="/Users/kim/IdeaProjects/SparkTreaning0822/src/main/resources/movie/rating_100k.data"

    // 打开文件
    val rdd = spark.sparkContext.textFile(file)

    // rdd => df  case class反射
    val df = rdd.mapPartitions(it => {
      it.map(line => {
        val contents = line.split("\t")
        MovieRating(
          Integer.parseInt(contents(0)),
          Integer.parseInt(contents(1)),
          Integer.parseInt(contents(2)),
          contents(3)
        )
      })
    }).toDF()

//    df.printSchema()
//    df.show()

    // SQL
    // 1. 注册临时视图
    df.createOrReplaceTempView("movie_rating")

    // 2. sql
    val sql=
      """
        |select movieID,round(avg(rating),2) as avg_rating,count(movieID) as rating_count from movie_rating
        |group by movieID having rating_count > 200
        |order by avg_rating desc,rating_count desc limit 10
        |""".stripMargin

    // 3. 执行sql
    val top10MovieRatingBySQL = spark.sql(sql)

    top10MovieRatingBySQL.show()

    println("=====================================")

    // DSL
    val top10MovieRatingByDSL = df.select($"movieID", $"rating")
      .groupBy($"movieID")
      .agg(
        round(avg($"rating"), 2).as("avg_rating"),
        count($"movieID").as("rating_count")
      ).filter("rating_count > 200").orderBy($"avg_rating".desc, $"rating_count".desc).limit(10)

    top10MovieRatingByDSL.show()

    // 将df保存到MySQL
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    properties.setProperty("user","dba")
    properties.setProperty("password","123456")

    top10MovieRatingBySQL.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://hadoop:3306/movie?characterEncoding=utf-8","movie_rating",properties)

    spark.stop()
  }

}
