package org.potato.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**Direct方式没有跑通，怀疑是kafka与spark版本兼容的问题，
  * 错误信息：java.lang.NoSuchMethodError: kafka.api.TopicMetadata.errorCode()S
  * Created by potato on 2017/12/27.
  */
object WordCountKafkaDirect {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val broker = "search-1:9092"
    val kafkaParams = Map("bootstrap.servers" -> broker)
    val topics = "wordcount".split(",").toSet
    val spark = SparkSession.builder.master("yarn-client").appName("WordCountKafka").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint("hdfs:///data/checkpoint")
    val kafkaDirectStream =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val pairs = kafkaDirectStream.map(_._2).flatMap(_.split(" ")).map(a => (a, 1))
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
