package org.potato.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by potato on 2017/12/27.
  */
object WordCountSocketTest {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val spark = SparkSession.builder.master("yarn-client").appName("SteamingWordCountTest").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    ssc.checkpoint("hdfs:///data/checkpoint")
    val textStream = ssc.socketTextStream("www-1", 9999)
    //通过socket 的方式接受流数据，这里通过socktTextStream创建流，第一个参数是监听的主机地址，第二个参数是监听的端口号。
    //需要在www-1的机器上安装nc工具 ，yum install nc 安装nc 工具 ，nc -lk 9999 启动9999端口
    val lines = textStream
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(a => (a, 1))
    val reduced = pairs.reduceByKey(_+_)
    reduced.foreachRDD(rdd=>{
      println(rdd.collect().toList)
    })
    //reduced.print()
    
//    /**使用updataStateByKey方法，必须开启checkpoint*/
//    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
//    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
