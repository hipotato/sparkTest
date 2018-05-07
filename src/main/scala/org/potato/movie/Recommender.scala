package org.potato.movie

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.potato.movie.caseClass.Movies
/**
  * Created by potato on 2017/12/25.
  */
object Recommender {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("Recommender").enableHiveSupport().getOrCreate()
    import spark.sql

      val traningRdd = sql("select distinct(userId) from traningData order by userId asc")


      val users  = traningRdd.take(21)

      for(user<-users){
        val userId = user.getInt(0)
        val modelPath = "/data/BestModle/0.7497439495699231"

        val model =MatrixFactorizationModel.load(spark.sparkContext,modelPath)

        val rec = model.recommendProducts(userId,5)

        val movies = rec.map(_.product)
        println(s"我为用户${userId}推荐了5部电影：")

        val movieName =movies.map(movie=>{
          val df = sql(s"select title from movies where movieid = ${movie}")
          df.first().getString(0)
        })
        movieName.foreach(x=>println(x))
      }
  }
}
