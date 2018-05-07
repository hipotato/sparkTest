package org.potato.movie

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer._

/**
  * Created by potato on 2017/12/27.
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic ="movie"
    val prop = new Properties()

    prop.put("bootstrap.servers", "www-1:9092")
    prop.put("acks", "all")
    prop.put("retries", "0")
    prop.put("batch.size", "16384")
    prop.put("linger.ms", "1")
    prop.put("buffer.memory", "33554432")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val spark=SparkSession.builder.master("yarn-client").appName("Recommender").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val testDF = spark.sql("select userId,movieId,rating from testData").toDF()
    val testData = testDF.map(x=>(topic,x.getInt(0).toString+"|"+x.getInt(1).toString+"|"+x.getDouble(2).toString))
    val producer = new KafkaProducer[String,String](prop)
    val messages = testData.toLocalIterator()
    while (messages.hasNext){
      val message = messages.next()
      val record = new ProducerRecord[String,String](topic,message._1,message._2)
      println(record)
      producer.send(record)
      Thread.sleep(500)
    }
  }
}
