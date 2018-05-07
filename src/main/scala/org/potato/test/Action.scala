package org.potato.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by potato on 2017/12/12.
  */
object Action {

  def main(args: Array[String]): Unit = {
    countByKey()
  }

  def reduce(): Unit ={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)

    val numList = Array(1,2,3,4,5,6,7,8,9,10)
    val nums = sc.parallelize(numList)
    val reduce1: Int = numList.reduce(_+_)
    println("sum="+reduce1)
  }

  def collect(): Unit ={
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9,10)
    val nums = sc.parallelize(numList)
    val doubled = nums.map(a=>a*2)
    val doubleArray =doubled.collect()
    for(a <-doubleArray )
      println("a")
  }
  def count(): Unit ={
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9,10)
    val nums = sc.parallelize(numList)
    val count = nums.count()
    println("count = "+count)
  }
  //获取前两个最大的数据
  def take(): Unit ={
    val conf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9,10)
    val nums = sc.parallelize(numList).sortBy(x=>x,false).take(2)
    for(a <-nums )
      println(a)

  }
  def countByKey(): Unit ={
    val conf = new SparkConf().setAppName("countByKey").setMaster( "local")
    val sc = new SparkContext(conf)
    val scoreList = Array(("class1",80),("class1",60),("class2",90),("class2",72),("class3",82))
    val scores = sc.parallelize(scoreList,1)
    val key: Map[String, Long] = scores.countByKey()
    for(a<-key){
      println(a)
    }


  }
}
