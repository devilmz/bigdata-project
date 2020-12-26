package com.zfk

import org.apache.spark.launcher.SparkLauncher

object TbdsSparkTest2 {
  def main(args: Array[String]): Unit = {
    val spark = new SparkLauncher()
      .setAppName("TbdsSparkTest2")
      .setSparkHome("/usr/hdp/2.2.0.0-2041/spark")
      .setMaster("yarn")
//      .setConf("spark.driver.memory", "1g")
//      .setConf("spark.executor.memory", "3g")
//      .setConf("spark.executor.cores", "2")
//      .setConf("spark.executor.instances", "2")
      .setConf("hive.metastore.uris", "thrift://tbds-172-16-0-5:9083")
      //指定hive的warehouse目录
      .setConf("spark.sql.warehouse.dir", "hdfs://hdfsCluster/apps/hive/warehouse")
      .setAppResource("spark-demo-1.0-SNAPSHOT.jar")
      .setMainClass("com.zfk.TbdsSparkTest")
      .setDeployMode("client")
      .startApplication()

    Thread.sleep(10000)
  }
}
