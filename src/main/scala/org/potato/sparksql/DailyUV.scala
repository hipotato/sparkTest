package org.potato.sparksql


import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by potato on 2017/12/30.
  */

object DailyUV {
  val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()
  case class Info(val date:String,val userId:Int)
  import spark.implicits._
  val userAccessLog = Array(
    "2017-12-30,1222",
    "2017-12-30,1201",
    "2017-12-31,1222",
    "2017-12-30,1224",
    "2017-12-31,1238",
    "2017-12-30,1002",
    "2017-12-30,1751",
    "2017-12-30,1201",
    "2017-12-31,1222",
    "2017-12-30,1222"
  )
  val userAccessLogRDD = spark.sparkContext.parallelize(userAccessLog,4)
  //转成Row的RDD
  val userAccessRowRDD = userAccessLogRDD.map(row=>Row(row.split(",")(0),row.split(",")(1).toInt))
  //构建DataFrame元数据
  val structType = StructType(Array(
    StructField("date",StringType,true),
    StructField("userId",IntegerType,true)
  ))
  //构建DataFrame
  val dataFrame  =  spark.createDataFrame(userAccessRowRDD,structType)
  //用data来分组，用userId去重统计总数
//  val aggRdd = dataFrame.groupBy("date").agg('date,countDistinct('userId)).rdd
//  aggRdd.map(row=>Row(row(0),row(2))).collect().foreach(println)
  val aggRdd = dataFrame.groupBy("date").agg('date,countDistinct('userId)).rdd
  aggRdd.map(row=>Row(row(0),row(2))).collect().foreach(println)
}
