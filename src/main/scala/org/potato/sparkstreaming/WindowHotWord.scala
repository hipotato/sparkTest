package org.potato.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** 一段时间内的搜索热点，搜索词格式：userName word
  * Created by potato on 2017/12/27.
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("yarn-client").appName("WindowHotWord").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint("hdfs:///data/checkpoint")
    //通过socket 的方式接受流数据，这里通过socktTextStream创建流，第一个参数是监听的主机地址，第二个参数是监听的端口号。
    //需要在www-1的机器上安装nc工具 ，yum install nc 安装nc 工具 ，nc -lk 9999 启动9999端口
    val lines = ssc.socketTextStream("www-1", 9999)
    val words = lines.map(_.split(" ")(1))
    val pairs = words.map(a => (a, 1))

    val searchWordcountStream = pairs.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Seconds(60),Seconds(10))
    //searchWordcountStream在60秒中有多个rdd，transform方法要对这其中的每个Rdd进行变换操作
    val finalDStream = searchWordcountStream.transform(wordCountRdd=>{
      val sortedWordCountRdd = wordCountRdd.map(tuple=>(tuple._2,tuple._1)).sortByKey(false).map(tuple=>(tuple._1,tuple._2))
      val top3SearchWordCounts = sortedWordCountRdd.take(2)
      for(tuple <- top3SearchWordCounts) {
        println(tuple)
      }
      wordCountRdd
    })
    finalDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
