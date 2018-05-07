package org.potato.movie

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by potato on 2017/12/28.
  */
object PopularMovies {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").enableHiveSupport().getOrCreate()

    val pop = spark.sql("select count(*) as c,movieId from traningdata group by movieId order by c desc")
    val top5 = pop.select("movieId").limit(5)
    top5.createOrReplaceTempView("top5")
    val pop5Result = spark.sql("select m.movieId, m.title from movies m join top5 t where (m.movieId=t.movieId)")
    pop5Result.write.mode(SaveMode.Overwrite).parquet("/hive/pop5")
    spark.sql("drop table if exists pop5")
    spark.sql("create table if not exists pop5(movieId int,title string) stored as parquet")
    spark.sql("load data inpath '/hive/pop5' overwrite into table pop5")
  }
}
