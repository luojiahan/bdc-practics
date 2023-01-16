package cn.un.kafka.util

import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql._

class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection:Connection = _
  var statement:Statement = _

  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }
  //https://blog.csdn.net/kt1776133839/article/details/90314604
  def process(value: Row): Unit = {
    statement.executeUpdate("INSERT INTO i_iots " +
      "VALUES ('" + value.getAs[String](0) + "','" + value.getAs[String](1) + "','" + value.getAs[Long](2) + "','" + value.getAs[Double](3) + "')")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
//使用
//val url="jdbc:mysql://hdp103:3306/test?characterEncoding=utf8&useSSL=false"
//val user ="root"
//val pwd = "199037"
//val writer = new JDBCSink(url,user, pwd)
//
//resultStreamDF.writeStream
//.foreach(writer)
//.outputMode("update")
//.trigger(ProcessingTime("25 seconds"))
//.start()
//.awaitTermination()