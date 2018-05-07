package org.potato.sparksql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by potato on 2017/12/29.
  */
object StudentJoin {
//  val spark = SparkSession.builder.master("yarn-client").appName("StudentJoin").getOrCreate()
  //
  //  import spark.sql
  //
  //  val top5DF = sql("select count(*) as c,movieId from traningdata group by movieId order by c desc limit 5")
  //
  //
  //  val movieIdRatingTimesRdd = top5DF.map(row => (row.getInt(1), row.getLong(0))).rdd
  //  val movieDF = sql("select movieId,title from movies")
  //
  //  val movieIdMovieTitleRdd = movieDF.map(row => (row.getInt(0), row.getString(1))).rdd
  //
  //  val join = movieIdRatingTimesRdd.join(movieIdMovieTitleRdd)
  //
  //  val joinedRdd = join.map(row => Row(row._1, row._2._2, row._2._1))
  //  val structType = StructType(Array(StructField("movieId", IntegerType), StructField("title", StringType), StructField("ratingCount", LongType)))
  //
  //  val df = spark.createDataFrame(joinedRdd, structType)
  //  df.write.format("json").save("hdfs:///data/top5Movies.json")

}



