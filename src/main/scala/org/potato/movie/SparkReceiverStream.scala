package org.potato.movie

import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by potato on 2017/12/27.
  */
object SparkReceiverStream {
  def main(args: Array[String]): Unit = {
    val batchDuration = new Duration(5)
    val spark=SparkSession.builder.master("yarn-client").appName("SparkDirectStream").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val validusers = spark.sql("select * from traningdata")
    val userlist = validusers.select("userId")
    val modelPath = "/data/BestModle/0.8503204903179099"
    val model =MatrixFactorizationModel.load(spark.sparkContext,modelPath)
    val zkQuorum ="search-1:2181"
    val topics =Map("potato"->1)
    def exist(u:Int):Boolean = {
      val userList = spark.sql("select distinct(userid) from traningdata").rdd.map(x => x.getInt(0)).collect()
      userList.contains(u)
    }
    def recommendPopularMovies() = {
      spark.sql("select * from top5").show
    }
    val groupId = "1"
    val storageLevel = StorageLevel.MEMORY_AND_DISK_2

    val kafkaDirectStream = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics,storageLevel)
    val messages = kafkaDirectStream.foreachRDD(rdd => {
      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelPath)
      val userRdd = rdd.map(x => x._2.split("|")).map(x => x(0)).map(_.toInt)
      val validUser = userRdd.filter(user => exist(user))
      val newUserRDD = userRdd.filter(user => !exist(user))
      val localIterator: Iterator[Int] = validUser.toLocalIterator
      while(localIterator.hasNext){
        val recresult = model.recommendProducts(localIterator.next(), 5)
        println("below movies are recommended for you :")
        println(recresult)
      }
    })
//      newUserRDD.foreachPartition(partition =>
//        while (partition.hasNext) {
//          val recresult = model.recommendProducts(partition.next(), 5)
//          println("below movies are recommended for you :")
//          recommendPopularMovies()
//        })

    ssc.start()
    ssc.awaitTermination()
  }
}
