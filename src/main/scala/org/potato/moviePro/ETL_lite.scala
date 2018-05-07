package org.potato.moviePro

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.potato.movie.caseClass._

/**
  * Created by potato on 2017/12/22.
  */
object ETL_lite {

  def main(args: Array[String]): Unit = {
    //val warehouseLocation = "spark-warehouse"
   // val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    val spark=SparkSession.builder.appName("DataFrameCreate").enableHiveSupport().getOrCreate()

    //导入包，支持把一个RDD隐式转换为一个DataFrame
    import spark.implicits._
    import spark.sql

    val linksDF = spark.read.textFile("/data/movieLenLite/links.csv").map(_.split(",")).filter(_.length==3).map(b=>Links(b(0).trim.toInt,b(1).trim.toInt,b(2).trim.toInt)).toDF()

    val movies = spark.sparkContext.textFile("/data/movieLenLite/movies.csv", 8).filter { !_.endsWith(",") }.map(_.split(",")).map(x => Movies(x(0).trim().toInt, x(1).trim(), x(2).trim())).toDF()

    val ratings = spark.sparkContext.textFile("/data/movieLenLite/ratings.csv", 8).filter { !_.endsWith(",") }.map(_.split(",")).map(x => Ratings(x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toDouble, x(3).trim().toInt)).toDF()

    val tags = spark.sparkContext.textFile("/data/movieLenLite/tags.csv", 8).filter { !_.endsWith(",") }.map(x=>rebuild(x)).map(_.split(",")).map(x => Tags(x(0).trim().toInt, x(1).trim().toInt, x(2).trim(), x(3).trim().toInt)).toDF()

    linksDF.write.mode(SaveMode.Overwrite).parquet("/hive/lite/links")
    sql("drop table if exists links_lite")
    sql("create table if not exists links_lite(movie int,imdbId int,tmdbId int) stored as parquet")
    sql("load data inpath '/hive/lite/links' overwrite into table links_lite")

    //movies
    movies.write.mode(SaveMode.Overwrite).parquet("/hive/lite/movies")
    sql("drop table if exists movies_lite")
    sql("create table if not exists movies_lite(movieId int,title string,genres string) stored as parquet")
    sql("load data inpath '/hive/lite/movies' overwrite into table movies_lite")

    //
    ratings.write.mode(SaveMode.Overwrite).parquet("/hive/lite/ratings")
    sql("drop table if exists ratings_lite")
    sql("create table if not exists ratings_lite(userId int,movieId int,rating double,timestamp int) stored as parquet")
    sql("load data inpath '/hive/lite/ratings' overwrite into table ratings_lite")

    //tags
    tags.write.mode(SaveMode.Overwrite).parquet("/hive/lite/tags")
    sql("drop table if exists tags_lite")
    sql("create table if not exists tags_lite(userId int,movieId int,tag string,timestamp int) stored as parquet")
    sql("load data inpath '/hive/lite/tags' overwrite into table tags_lite")


    sql("drop table if exists mite_content_base_tmp")
    sql("create table if not exists mite_content_base_tmp(movieid int,movieid_recommand int,score double)")

    sql("drop table if exists mite_content_base_reco")
    sql("create table if not exists mite_content_base_reco(movieid int,movieid_recommand int,score double)")

  }
  def rebuild(input:String):String = {
    val a = input.split(",")
    val head = a.take(2).mkString(",")
    val tail = a.takeRight(1).mkString
    val b = a.drop(2).dropRight(1).mkString.replace("\"", "")
    val output = head + "," + b + "," + tail
    output
  }
}
