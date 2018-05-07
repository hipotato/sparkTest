package org.potato.movie

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.potato.movie.caseClass._

/**
  * Created by potato on 2017/12/22.
  */
object etl_likes {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.appName("DataFrameCreate").enableHiveSupport().getOrCreate()

    //导入包，支持把一个RDD隐式转换为一个DataFrame
    import spark.implicits._
    import spark.sql

    val likes = spark.sparkContext.textFile("file:///home/hbase/spark/article_likes.txt", 8).filter { !_.endsWith(",") }.map(_.split(",")).map(x => Likes(x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toDouble)).toDF()

    likes.write.mode(SaveMode.Overwrite).parquet("/hive/likes")
    sql("drop table if exists likes")
    sql("create table if not exists likes(userId int,articleId int,like double) stored as parquet")
    sql("load data inpath 'hdfs://www-1:9000/hive/likes' overwrite into table likes")

  }

}
