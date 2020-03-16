package com.zhangweiwhim

import java.time.LocalDate.now
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
/**
 * Description: wk_batch
 * Created by zhangwei on 2020/2/20 13:34
 */
object RepayCalc {

  def main(args: Array[String]): Unit = {

    // 命令行参数
    val exec_date = if (args.length == 0) now
    println(s"exec_date:\t${exec_date}")

    val spark = SparkSession
      .builder()
      .appName("大数据端逾期信息表wk_repay_calc")
      .enableHiveSupport()
      .getOrCreate()

    val query =Source.fromInputStream(this.getClass.getResourceAsStream("/query/repay_calc/common.sql")).getLines().mkString("\n")
    // 执行日期
    spark.sql(s"set exec_date = '${exec_date}'")
    // wk表
    val calcDF = spark.sql(query).cache()
    // 数据库连接信息
    val properties = new Properties()
    val path = this.getClass.getResourceAsStream("/conf.properties") // Thread.currentThread().getContextClassLoader.getResource("/conf.properties").getPath //文件要放到resource文件夹下
    properties.load(path)
    val url = properties.getProperty("mysql.db.url")
    val table = properties.getProperty("mysql.output.table")
    val connectionProperties = new Properties()
    connectionProperties.put("user", properties.getProperty("mysql.user"))
    connectionProperties.put("password", properties.getProperty("mysql.password"))

    // wk表写入数据库
    calcDF.write.option("truncate", "true").mode(SaveMode.Overwrite).jdbc(url, table, connectionProperties)
    spark.stop()
  }
}
