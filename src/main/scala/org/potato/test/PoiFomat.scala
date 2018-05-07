package org.potato.test

import org.apache.spark.{SparkConf, SparkContext}

object PoiFomat {
    def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PoiFomat").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val filted = lines.filter(a=>a.indexOf("coordinates")>=0&&a.indexOf("]",a.indexOf("coordinates")+17)>=0)
    val maped = filted.map(a=>a.substring(a.indexOf("coordinates")+17,a.indexOf("]",a.indexOf("coordinates")+17)))
      maped.coalesce(1).saveAsTextFile(args(1))
  }

}
