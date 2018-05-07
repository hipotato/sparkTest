package org.potato.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by potato on 2017/12/28.
  */
object ParquetLoadSave {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()
    //从parquet文件中读取至DF中
    val moviesDF = spark.read.load("/hive/movies")
    val movieIdDF = moviesDF.select("movieId")
    //从DF中写数据至parquet文件中
    movieIdDF.write.save("hdfs:///hive/movieId")

  }
}
