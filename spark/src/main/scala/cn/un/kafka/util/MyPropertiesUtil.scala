package cn.un.kafka.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 读取配置文件工具类
 */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    val broker_list = prop.getProperty("kafka.broker.list")

    println(broker_list)

  }
  def load(propertiesName:String): Properties ={
    val prop: Properties = new Properties()
    //加载制定的配置文件:配置文件打包后到target的class里边
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    prop

  }
}
