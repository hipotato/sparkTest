package org.potato.dataset

import org.apache.spark.sql.SparkSession


/**
  * Created by potato on 2017/12/27.
  */
object DataSetFromJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("wordCountDataSet").getOrCreate()
    val df = spark.read.json("file:////home/hbase/spark/toImport20180119.txt")
   df.show();
  }
}
