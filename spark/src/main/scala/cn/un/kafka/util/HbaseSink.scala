package cn.un.kafka.util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row}

class HbaseSink extends ForeachWriter[Row] {
  var table: Table = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    //获取连接
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "hdp101,hdp102,hdp103")
    val conn: Connection = ConnectionFactory.createConnection(conf)
    table = conn.getTable(TableName.valueOf("t_iots"))
    true
  }

  override def process(value: Row): Unit = {
    //create 't1','iot_info'
    //rowkey:调度编号+车牌号+时间戳
    //var rowkey = value.deployNum + value.plateNum + value.timeStr
    var rowkey = value.getAs[String](0)
    val put = new Put(Bytes.toBytes(rowkey))
    // val arr: Array[String] = value.lglat.split("_")
    //device_type
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("device_type"),
      Bytes.toBytes(value.getAs[String](1))
    )
    //count_device
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("count_device"),
      Bytes.toBytes(value.getAs[Long](2))
    )
    //avg_signal
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("avg_signal"),
      Bytes.toBytes(value.getAs[Double](3))
    )
    table.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    table.close()
  }
}
//5、启动流失应用，结果输出到HBase
//resultStreamDF.writeStream
//.foreach(new HbaseSink)
//.outputMode("append")
//.start()
//.awaitTermination()