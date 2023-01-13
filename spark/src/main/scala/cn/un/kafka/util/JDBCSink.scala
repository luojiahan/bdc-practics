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

  def process(value: Row): Unit = {
    statement.executeUpdate("INSERT INTO i_iots " +
      "VALUES (" + value.getString(0) + "," + value.getString(1) + "," + value.getLong(2) + "," + value.getDouble(3) + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
//使用
//val url="jdbc:mysql://hdp103:3306/test"
//val user ="user"
//val pwd = "pwd"
//val writer = new JDBCSink(url,user, pwd)
//
//val query = resultStreamDF
//.writeStream
//.foreach(writer)
//.outputMode("update")
//.trigger(ProcessingTime("25 seconds"))
//.start()