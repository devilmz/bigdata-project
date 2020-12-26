package com.zfk

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object SparkStreamKafkaDemo {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val kafkaBroker = args(0)
    val groupid = args(1)
    val authid = args(2)
    val authkey = args(3)
    val topic = args(4)

    val sparkConf = new SparkConf().setAppName("SparkStreamKafkaDemo")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "3000",
      "security.protocol" -> "SASL_TBDS",
      "sasl.mechanism" -> "TBDS",
      "sasl.tbds.secure.id" -> authid,
      "sasl.tbds.secretId" -> authid,
      "sasl.tbds.secure.key" -> authkey
    )

    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(topic), kafkaParams)
    )

    dStream.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        logger.info("================== no streaming data found ==================")
      } else {
        rdd.foreach(record => logger.info("===================>>>>>>" + record.value()))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
