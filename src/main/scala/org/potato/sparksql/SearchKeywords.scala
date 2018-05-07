package org.potato.sparksql

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
/**
  * 统计出每天搜索uv排名前3的搜索词
  * Created by potato on 2017/12/30.
  */
object SearchKeywords {
  val spark=SparkSession.builder.master("yarn-client").appName("SearchKeywords").getOrCreate()
  import spark.sql
  import spark.implicits._

  val originRdd = spark.sparkContext.textFile("/data/search.txt").map(line=>line.split("\t")).filter(_.size==5).filter(a=>a(4)=="android").map(a=>Row(a(0)+" "+a(2),a(1)))

  val structType = StructType(Array(
    StructField("date_keyword",StringType,true),
    StructField("user",StringType,true)
  ))

  val dataFrame = spark.createDataFrame(originRdd,structType)

  //创建keywords表
  dataFrame.createOrReplaceTempView("keywords")
  /**计算每天排名前三的keywords */
    val df = spark.sql("select date_keyword,count(distinct user) as vu from keywords group by date_keyword ")
  val newRdd =df.rdd.map(row=>Row(row(0).toString.split(" ")(0),row(0).toString.split(" ")(1),row.getLong(1)))
  val structType2 = StructType(Array(
    StructField("date",StringType,true),
    StructField("keyword",StringType,true),
    StructField("uv",LongType,true)
  ))
  val dataFrame2 = spark.createDataFrame(newRdd,structType2)
  //创建keywords表
  dataFrame2.createOrReplaceTempView("keywords2")
  val top3SalesDF =  spark.sql(""
    + "SELECT date,keyword,uv "
    + "FROM ("
    + "SELECT "
    + "date,"
    + "keyword,"
    + "uv,"
    + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
    + "FROM keywords2 "
    + ") tmp "
    + "WHERE rank<=3")
  //dataFrame转为临时表
  top3SalesDF.createOrReplaceTempView("top3Keywords")
  //存到hive中
  sql("create table top3Keywords as select * from top3Keywords")
}
