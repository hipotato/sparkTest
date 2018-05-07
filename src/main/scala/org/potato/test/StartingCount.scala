package org.potato.test

import org.apache.spark.{SparkConf, SparkContext}

object StartingCount {
    def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StartingCount").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val filted = lines.filter(a=>a.indexOf("starting")>=0&&a.indexOf("\"",a.indexOf("starting")+15)>=0)
    val maped = filted.map(a=>a.substring(a.indexOf("starting")+15,a.indexOf("\"",a.indexOf("starting")+15)))
      var startingAndOne = maped.map((_,1))
      val reduced = startingAndOne.reduceByKey(_+_)
      reduced.coalesce(1).saveAsTextFile(args(1))
      sc.stop()
    }

}
