package org.potato.movie

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.potato.movie.caseClass.{Links, Movies, Ratings, Tags}

/**
  * Created by potato on 2017/12/23.
  */
object RatingData {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").enableHiveSupport().getOrCreate()

    //导入包，支持把一个RDD隐式转换为一个DataFrame
    import spark.implicits._
    import spark.sql


    val count = sql("select count(*) from ratings").first().getLong(0).toInt
    val precent = 0.7
    val traningCount = (count * precent).toInt
    val testCount = (count*(1-precent)).toInt

    val ratingDesc = sql("select userId,movieId,rating from ratings order by timestamp desc")
    val ratingAsc = sql("select userId,movieId,rating from ratings order by timestamp asc")
    //ratingDesc
    ratingDesc.write.mode(SaveMode.Overwrite).parquet("/hive/ratingdesc")
    ratingAsc.write.mode(SaveMode.Overwrite).parquet("/hive/ratingasc")
    sql("drop table if exists ratingdesc")
    sql("create table if not exists ratingdesc(userId int,movieId int,rating double) stored as parquet")
    sql("load data inpath '/hive/ratingdesc' overwrite into table ratingdesc")

    sql("drop table if exists ratingasc")
    sql("create table if not exists ratingasc(userId int,movieId int,rating double) stored as parquet")
    sql("load data inpath '/hive/ratingasc' overwrite into table ratingasc")

    val traningdata =sql(s"select * from  ratingdesc limit ${traningCount}")
    traningdata.write.mode(SaveMode.Overwrite).parquet("/hive/traningdata")
    val testdata = sql(s"select * from  ratingasc limit ${testCount}")
    testdata.write.mode(SaveMode.Overwrite).parquet("/hive/testdata")
    sql("drop table if exists traningdata")
    sql("create table if not exists traningdata(movieId int,userId int,rating double) stored as parquet")
    sql("load data inpath '/hive/traningdata' overwrite into table traningdata")

    sql("drop table if exists testdata")
    sql("create table if not exists testdata(movieId int,userId int,rating double) stored as parquet")
    sql("load data inpath '/hive/testdata' overwrite into table testdata")
  }
}
