package org.potato.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by potato on 2017/12/27.
  */
object WordCountKafkaReciver {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val zkQuorum ="search-1:2181"
    val topics =Map("wordcount"->1)
    val spark = SparkSession.builder.master("yarn-client").appName("WordCountKafka").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val groupId = "1"
    val storageLevel = StorageLevel.MEMORY_AND_DISK_2
    ssc.checkpoint("hdfs:///data/checkpoint")
    val kafkaDirectStream = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics,storageLevel)
    val pairs = kafkaDirectStream.map(_._2).flatMap(_.split(" ")).map(a => (a, 1))
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
