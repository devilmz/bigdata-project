package com.zfk

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data1 = env.socketTextStream("localhost", 9999)
    val count = data1.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)

    count.writeAsText("StreamWordCount.txt",WriteMode.OVERWRITE)

    env.execute("StreamWordCount")
  }

}
