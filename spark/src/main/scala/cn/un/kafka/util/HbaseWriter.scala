package cn.un.kafka.util

import cn.un.kafka.IotStreamingOnlineSQL.{IotInfo, IotStaticInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.ForeachWriter

object HbaseWriter {
  //hbase中的connection本身底层已经使用了线程池，而且connection是线程安全的，可以全局使用一个，
  //但是对admin,table需要每个线程使用一个

  def getHtable(): Table = {
    //获取连接
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "hdp101,hdp102,hdp103")
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //hbase表名：htb_gps
    val table: Table = conn.getTable(TableName.valueOf("t_iots"))
    table
  }
}


class HbaseWriter extends ForeachWriter[IotStaticInfo] {
  var table: Table = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    table = HbaseWriter.getHtable()
    true
  }

  override def process(value: IotStaticInfo): Unit = {
    //rowkey:调度编号+车牌号+时间戳
    //var rowkey = value.deployNum + value.plateNum + value.timeStr
    var rowkey = value.device_id
    val put = new Put(Bytes.toBytes(rowkey))
    // val arr: Array[String] = value.lglat.split("_")
    //device_type
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("device_type"),
      Bytes.toBytes(value.device_type)
    )
    //count_device
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("count_device"),
      Bytes.toBytes(value.count_device)
    )
    //avg_signal
    put.addColumn(
      Bytes.toBytes("iot_info"),
      Bytes.toBytes("avg_signal"),
      Bytes.toBytes(value.avg_signal)
    )
    table.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    table.close()
  }
}
