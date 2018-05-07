package org.potato.moviePro

import com.hankcs.hanlp.HanLP
import com.potato.moviePro.{CosineSimilarAlgorithm, movieYearRegex}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import collection.convert.wrapAll._
import scala.util.control.Breaks


/** 基于内容的推荐
  * Created by potato on 2018/5/3.
  */
object ContentBasedRec {

  /**
    * 同义词合并判断
    * @param str1 较短的同义词
    * @param str2 较长的同义词
    * @return 是否合并标识：1：合并，2：不合并
    */
  def getEditSize(str1:String,str2:String): Int ={
    if (str2.size > str1.size){
      0
    } else {
      //计数器
      var count = 0
      val loop = new Breaks
      //以较短的str2为中心，进行遍历，并逐个比较字符
      val lengthStr2 = str2.getBytes().length
      var i = 0
      for ( i <- 1 to lengthStr2 ){
        if (str2.getBytes()(i) == str1.getBytes()(i)) {
          //逐个匹配字节，相等则计数器+1
          count += 1
        } else {
          //一旦出现前缀不一致则中断循环，开始计算重叠度
          loop.break()
        }
      }
      //计算重叠度,当前缀重叠度大于等于2/7时，进行字符串合并，从长的往短的合并
      if (count.asInstanceOf[Double]/str1.getBytes().size.asInstanceOf[Double] >= (1-0.286)){
        1
      }else{
        0
      }
    }
  }

  /**
    * 计算cosin相似度，基础算法
    * @param first 排序好的 单词，次数
    * @param second
    * @return
    */
  def calculateCos(first:Map[String, Int],second:Map[String, Int]):Double ={

    val firstVector = first.toVector
    val secondVector  = second.toVector;

    var vectorFirstModulo = 0.00 //向量1的模
    var vectorSecondModulo = 0.00 //向量2的模

    var vectorProduct =0.00 //向量积
    val secondSize = secondVector.size
    for(i <- 0 until firstVector.size){
        if(i<secondSize){
          vectorSecondModulo = vectorSecondModulo + secondVector(i)._2*secondVector(i)._2
          vectorProduct = vectorProduct + firstVector(i)._2*secondVector(i)._2
        }
      vectorFirstModulo = vectorFirstModulo+ firstVector(i)._2*firstVector(i)._2
    }
    vectorProduct / (Math.sqrt(vectorFirstModulo) * Math.sqrt(vectorSecondModulo))
  }

  def calculateCos(first:List[String],second:List[String]):Double ={
    calculateCos(first.map(a=>(a,1)).toMap,second.map(a=>(a,1)).toMap)
  }

  def calculateCos(first:java.util.List[String],second:java.util.List[String]):Double ={
    calculateCos(first.map(a=>(a,1)).toMap,second.map(a=>(a,1)).toMap)
  }

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName("base-content-Recommand").enableHiveSupport().getOrCreate()//spark任务名称

    import spark.implicits._
    /**三个表可以先load到Hive中，然后spark直接从Hive中读取，形成DataFrame*/
    //从hive中，获取rating评分数据集，最终形成如下格式数据(movie,avg_rate)
    val movieAvgRate = spark.sql("select movieId,round(avg(rating),1) as avg_rate  from ratings group by movieId").rdd.map{
      f=>
        (f.getInt(0),f.getDouble(1))
    }
    //获取电影的基本属性数据，包括电影id，名称，以及genre类别
    val moviesData = spark.sql("select movieId,title,genres from movies").rdd
    //获取电影tags数据，这里取到所有的电影tag
    val tagsData = spark.sql("select movieId,tag from tags").rdd

    val tagsSimi = spark.sql("select movieId,tag,count from tagsStandardize").rdd.map(a=>{
      ((a.getInt(0),a.getString(1)),a.getInt(2))
    })


    /**统计tag频度，取TopN个作为电影对应的tag属性*/
    val movieTag = tagsSimi.groupBy(k=>k._1._1).map{
      f=>
        (f._1,f._2.map{
          ff=>
            (ff._1._2,ff._2)
        }.toList.sortBy(_._2).reverse.take(10).toMap)
    }

    //println("movieTag的长度：-----------------------------------------------"+movieTag.count())

    /**关键词抽取*/
    val moviesGenresTitleYear = moviesData.map{
      f=>
        val movieid = f.getInt(0)
        val title = f.get(1)
        val genres = f.get(2).toString.split("|").toList.take(10)
        val titleWorlds = HanLP.extractKeyword(title.toString, 10)
        val year = movieYearRegex.movieYearReg(title.toString)
        (movieid,(genres,titleWorlds,year))
    }

    /**通过join进行数据合并，生成一个以电影id为核心的属性集合*/
    val movieContent = movieTag.join(movieAvgRate).join(moviesGenresTitleYear).map{
      f=>
        //(movie,tagList,titleList,year,genreList,rate)
        (f._1,f._2._1._1,f._2._2._2,f._2._2._3,f._2._2._1,f._2._1._2)
    }


    //println("movieContent的长度：-----------------------------------------------"+movieContent.count())


    val movieConetentTmp = movieContent.filter(f=>f._6 > 4.5).collect()

    /**使用余弦相似度计算，对每个电影计算出最相似的Top20电影*/
    val movieContentBase = movieContent.map{
      f=>
        val currentMoiveId = f._1
        val currentTagList = f._2 //Map[String, Int]
        val currentTitleWorldList = f._3
        val currentYear = f._4
        val currentGenreList = f._5
        val currentRate = f._6
        val recommandMovies = movieConetentTmp.map{
          ff=>
            val tagSimi = calculateCos(currentTagList,ff._2)
            val titleSimi = calculateCos(currentTitleWorldList,ff._3)
            val genreSimi = calculateCos(currentGenreList,ff._5)
            val yearSimi = calculateCos(List(currentYear.toString),List(ff._4.toString))
            val rateSimi = ff._6.doubleValue()/5.0
            val score = 0.4*genreSimi + 0.25*tagSimi + 0.1*yearSimi + 0.05*titleSimi + 0.2*rateSimi
            //val score = 0.5*genreSimi + 0.25*tagSimi + 0.05*titleSimi + 0.2*rateSimi
            (ff._1,score)
        }.toList.sortBy(k=>k._2).reverse.take(20)

        (currentMoiveId,recommandMovies)
    }.flatMap(f=>f._2.map(k=>(f._1,k._1,k._2))).map(f=>Row(f._1,f._2,f._3))

    //println("movieContentBase的长度：-----------------------------------------------"+movieContentBase.count())

    //println("movieContentBase:"+ movieContentBase.take(10))

    /**将结果写入hive表中*/
    //我们先进行DataFrame格式化申明
    val schemaString2 = "movieid movieid_recommand score"
    val schemaContentBase = StructType(schemaString2.split(" ")
      .map(fieldName=>StructField(fieldName,if (fieldName.equals("score")) DoubleType else  IntegerType,true)))

    case class Recs(movieid:Int,movieid_recommand:Int,score:Double)

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()

    val movieContentBaseDataFrame = spark.createDataFrame(movieContentBase,schemaContentBase)


    //println("DataFrame的长度：-----------------------------------------------"+count)

    //将结果存入hive,需要先进行临时表创建
    val userTagTmpTableName = "mite_content_base_tmp3"
    val userTagTableName = "mite_content_base_reco"
    movieContentBaseDataFrame.createOrReplaceTempView(userTagTmpTableName)
    spark.sql("truncate table " + userTagTableName)

    spark.sql("insert into table " + userTagTableName + " select * from " + userTagTmpTableName)
  }
}
