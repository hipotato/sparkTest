package org.potato.movie

import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * Created by potato on 2017/12/27.
  */
object SparkDirectStream {
  def main(args: Array[String]): Unit = {
    val batchDuration = new Duration(5)
    val spark = SparkSession.builder.master("yarn-client").appName("SparkDirectStream").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val validusers = spark.sql("select * from traningdata")
    val userlist = validusers.select("userId")
    val modelPath = "/data/BestModle/0.8503204903179099"
    val model = MatrixFactorizationModel.load(spark.sparkContext, modelPath)
    val broker = "search-1:9092"
    val topics = "222".split(",").toSet
    val kafkaParams = Map("bootstrap.servers" -> broker)

    def exist(u: Int): Boolean = {
      val userList = spark.sql("select distinct(userid) from traningdata").rdd.map(x => x.getInt(0)).collect()
      userList.contains(u)
    }

    def recommendPopularMovies() = {
      spark.sql("select * from top5").show
    }
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val messages = kafkaDirectStream.foreachRDD(rdd => {
      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelPath)
      val userRdd = rdd.map(x => x._2.split("|")).map(x => x(0)).map(_.toInt)
      val validUser = userRdd.filter(user => exist(user))
      val newUserRDD = userRdd.filter(user => !exist(user))
      validUser.foreachPartition(partition =>
        while (partition.hasNext) {
          val recresult = model.recommendProducts(partition.next(), 5)
          println("below movies are recommended for you :")
          println(recresult)
        }
      )
      newUserRDD.foreachPartition(partition =>
        while (partition.hasNext) {
          val recresult = model.recommendProducts(partition.next(), 5)
          println("below movies are recommended for you :")
          recommendPopularMovies()
        })
    })
  }
}
