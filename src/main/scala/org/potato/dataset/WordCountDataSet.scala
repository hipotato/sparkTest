package org.potato.dataset

import org.apache.spark.sql.SparkSession


/**
  * Created by potato on 2017/12/27.
  */
object WordCountDataSet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("wordCountDataSet").getOrCreate()
    import spark.implicits._
    val rdd = spark.read.textFile("file:///home/hbase/spark/spark-2.1.0-bin-hadoop2.6/README.md").as[String]
    val words = rdd.flatMap(a=>a.split(" "))
    var key = words.groupByKey(_.toLowerCase)
    val groupedWords = key.count()
    groupedWords.show()
  }
}
