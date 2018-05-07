package org.potato.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by potato on 2017/12/12.
  */
object Transformation {
  def main(args: Array[String]): Unit = {
    joinAndCogroup()
  }

  def groupByKey(): Unit ={
    var conf = new SparkConf().setAppName("GroupBy").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(("class1",80),("class1",60),("class2",90),("class2",72),("class3",82))
    val scores = sc.parallelize(scoreList,1)
    val grouped =  scoreList.groupBy(_._1)
    grouped.foreach(score=>{
      print(score._1+"-----")
      score._2.foreach(b=>print(b._2+" "))
      print("\n")
    })
  }
  def reduceByKey(): Unit ={
    var conf = new SparkConf().setAppName("GroupBy").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(("class1",80),("class1",60),("class2",90),("class2",72),("class3",82))
   val scores = sc.parallelize(scoreList,1)
   val totalScores =  scores.reduceByKey((a,b)=>a+b)
    totalScores.foreach(a=>println(a._1+":"+a._2))
  }
  def sortBy(): Unit ={
    var conf = new SparkConf().setAppName("GroupBy").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(("class1",80),("class1",60),("class2",90),("class2",72),("class3",82))
    val scores = sc.parallelize(scoreList,1)
    val sorted  =  scores.sortBy(x =>(-x._2),false)
    sorted.foreach(a=>println(a._1+":"+a._2))
  }
  def sortByKey(): Unit ={
    var conf = new SparkConf().setAppName("GroupBy").setMaster( "local")
    val sc = new SparkContext(conf)
    val scoreList = Array(("class1",80),("class1",60),("class2",90),("class2",72),("class3",82))
    val scores = sc.parallelize(scoreList,1)
    val reversed   = scores.map(a=>Tuple2(a._2,a._1))
    val sorted = reversed.sortByKey()
    sorted.foreach(a=>println(a._1+":"+a._2))
  }

  def joinAndCogroup(): Unit ={
    val conf = new SparkConf().setAppName("GroupBy").setMaster( "local")
    val sc = new SparkContext(conf)
    val studentList = Array((1,"potato"),(2,"tomato"),(3,"banana"))
    val scoreList = Array((1,100),(2,84),(3,90))
    val students = sc.parallelize(studentList,1)
    val scores = sc.parallelize(scoreList,1)
    val result: RDD[(Int, (String, Int))] = students.join(scores)
    val sorted = result.sortBy(_._2._2)
    sorted.foreach(a=>println(a._1+" "+a._2._1+" "+a._2._2))
  }
}
