package org.potato.test

import org.apache.spark.sql.SparkSession

/**
  * Created by potato on 2017/12/21.
  */
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()

    val df = sparkSession.read.option("header","true").csv(args(0))

    df.show()
    println("数据集总量："+ df.count())

  }

}
