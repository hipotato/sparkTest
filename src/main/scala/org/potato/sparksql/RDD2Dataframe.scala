package org.potato.sparksql

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by potato on 2017/12/28.
  */
object RDD2Dataframe {
  def main(args: Array[String]): Unit = {

//    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()
//    import spark.implicits._
//    val movies = spark.sparkContext.textFile("/data/movies.txt", 8).filter { !_.endsWith(",") }.map(_.split(",")).map(row=>Movies(row(0).toString.toInt,row(1).toString,row(2).toString))
//    val moviesDF = movies.toDF()
//    moviesDF.createOrReplaceTempView("movies2")
//
//    val top10MoivesRDD = spark.sql("select * from movies2 limit 10").rdd
//
//    top10MoivesRDD.map(row=>Movies(row(0).toString.toInt,row(1).toString,row(2).toString)).collect().foreach(
//      movie =>println(movie.movieId+"|"+movie.title+"|"+movie.genres)
//    )
//
//    //第二种方法
//    val movies2 = spark.sparkContext.textFile("/data/movies.txt", 8).filter { !_.endsWith(",") }.map(_.split(",")).map(line=>Row(line(0).toInt,line(1),line(2)))
//
//    val structType = StructType(Array(StructField("movieId",IntegerType,true),StructField("title",StringType,true),StructField("title",StringType,true)))
//
//    val movies2DF = spark.createDataFrame(movies2,structType)
//
//    val moviesDF = movies.toDF()
//    movies2DF.createOrReplaceTempView("movies2")
//
//    val top10MoivesRDD2 = spark.sql("select * from movies2 limit 10").rdd
//
//    top10MoivesRDD2.map(row=>Movies(row(0).toString.toInt,row(1).toString,row(2).toString)).collect().foreach(
//      movie =>println(movie.movieId+"|"+movie.title+"|"+movie.genres))
//
//    //第三种方法，从parquet中load
//    val movies3=spark.read.load("/hive/movies")
//    movies3.createOrReplaceTempView("movies3")
//    val top10MoivesTitle= spark.sql("select title from movies3 limit 10").rdd
//    top10MoivesTitle.collect().foreach(row=>println(row.getString(0)))

  }
  case class Movies(movieId:Int,title:String,genres:String)

}
