package org.potato.test
import org.apache.spark.sql.SparkSession

import scala.util.Success

/**
  * Created by potato on 2017/12/22.
  */
object Reflection {
  case class Movie(id:Int,title:String,genres:String)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("yarn-client").appName("Reflection").getOrCreate()
    //导入包，支持把一个RDD隐式转换为一个DataFrame
    import spark.implicits._
    //定义一个funtion
    def isInt(input:String):Boolean={
      val r1=scala.util.Try(input.toInt)
      val result = r1 match {
        case Success(_) => true ;
        case _ =>  false
      }
      result
    }
    val moviesDF = spark.sparkContext.textFile(args(0))
      .map(a => a.split(","))
      .filter(b=>isInt(b(0)))
      .map(b => Movie(b(0).trim().toInt, b(1), b(2)))
      .toDF()

    moviesDF.createOrReplaceTempView("movies")
    println("movies个数："+moviesDF.count())
    moviesDF.show(50)
  }

}
