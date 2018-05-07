package org.potato.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by potato on 2017/12/28.
  */
object ParquetMerge {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()
    //从parquet文件中读取至DF中
    val moviesDF = spark.read.load("hdfs:////hive/movies")

    val ratingDF = spark.read.load("hdfs:///hive/ratings")

    moviesDF.write.mode(SaveMode.Append).parquet("hdfs:///hive/merge/ratingExtra")
    ratingDF.write.mode(SaveMode.Append).parquet("hdfs:///hive/merge/ratingExtra")

    val ratingNew  = spark.read.option("mergeSchema","true").load("hdfs:///hive/merge/ratingExtra")
    ratingNew.limit(2).show()
  }
}
