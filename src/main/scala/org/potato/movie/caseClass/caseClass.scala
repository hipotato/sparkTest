package org.potato.movie.caseClass

/**
  * Created by potato on 2017/12/22.
  */
case class Links(movieId:Int,imdbId:Int,tmdbId:Int)

case class Movies(movieId:Int,title:String,genres:String)

case class Ratings(userId:Int,movieId:Int,rating:Double,timestamp:Int)

case class Tags(userId:Int,movieId:Int,tag:String,timestamp:Int)

case class Result(userId:Int,movieId:Int,rating:Double)

case class Likes(userId:Int,articleId:Int,like:Double)