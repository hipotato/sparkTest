package org.potato.scala

/**
  * Created by potato on 2018/1/4.
  */
object scalaTest {
  def main(args: Array[String]): Unit = {
    judegeGrade("A")
  }
  def judegeGrade(grade:String): Unit ={
    grade match {
      case "A" => println("excellent")
      case "B"=>println("good")
      case "C"=>println("Just So So")
      case _ =>println("you need worker harder")
    }
  }
}
