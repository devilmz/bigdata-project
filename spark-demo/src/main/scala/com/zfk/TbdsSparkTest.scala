package com.zfk

import org.apache.spark.sql.SparkSession

object TbdsSparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("tbdssparktest")
      .enableHiveSupport()
      .config("hbase.security.authentication.tbds.secureid","u2jsSUziz0ybHweQ76OxMjpsAm9l35AjoFof")
      .config("hbase.security.authentication.tbds.securekey","NRk5j9L4ZJRAv0PVwUJiU8yh9W8MAmvg")
      .getOrCreate()

    //
    //    spark.sql("create table testadmindb.t1(id int)")
    //    spark.sql("insert into testadmindb.t1 values(1),(2),(3)")
    //    spark.sql("alter table testadmindb.t1 rename to testadmindb.t2")

    val df1 = spark.sql("select * from admintestdb.t1")
//    df1.write.saveAsTable("testadmindb.t3")
    df1.show()

    spark.stop()
  }
}
