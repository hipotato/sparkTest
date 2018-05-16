package org.potato.poiRec

import com.potato.moviePro.{GeoNumUtil, TextSimilarity}
import com.potato.poiMatch.util.HttpRequestUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.JavaConversions._

/** 相似度计算
  * Created by potato on 2018/5/7.
  */
object poiMatch {
  def main(args: Array[String]): Unit = {
    //创建SparkSession

    val spark = SparkSession.builder().appName("poi_match").enableHiveSupport().getOrCreate()//spark任务名称

    var selectPoi = "select id,name,lat,lng,address,tel from no_match where lat is not null and lng is not null"

    case class Poi(id:String,name:String,lat:String,lng:String,address:String,tel:String)

    val poiRDD: RDD[Poi] = spark.sql(selectPoi).repartition(10000).rdd.map {
      f => Poi(f.getString(0),f.getString(1), f.getString(2), f.getString(3), f.getString(4), f.getString(5))
    }.filter(_.lat!=null).filter(_.lng!=null).filter(_.name!=null)
    /**从Hive中读取poi数据，形成DataFrame*/
    //从hive中，获取rating评分数据集，最终形成如下格式数据(movie,avg_rate)

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()
    val poiDf = poiRDD


    //val poiColleted = poiDf.collect()
    case class ToSave(id:String,poi:Poi,simi:Double)
    val result = poiDf.map(a =>{
      val selId = a.id
      val selName = a.name
      val selLng = a.lng
      val selLat = a.lat
      val location = a.lng+","+a.lat
      val telTel = a.tel

      val starTime = System.currentTimeMillis()
      println("起始时间:"+starTime)
      val nearByPois = HttpRequestUtil.getJiugongPois(location,18)
      val endTime = System.currentTimeMillis()
      println("结束时间:"+endTime+",用时："+(endTime-starTime)+",poi数量："+nearByPois.size())

      val simiPois  = nearByPois.map{
        ff=>
          val id =  ff.getBase.getId
          val name = ff.getBase.getName
          val geoNum = ff.getBase.getGeoNum
          val typecode = ff.getBase.getTypecode
          val location = ff.getBase.getLocation
          val address = ff.getBase.getAddress
          val telTemp = ff.getBase.getTel
          var tel="";
          if(telTemp!=null&&telTemp.size()>0){
            tel = telTemp.get(0)
          }

          //名称相似度
          val nameSim = TextSimilarity.similar(selName,name)

          //地址相似度
          val addresSim = TextSimilarity.similar(selName,name)

          //电话相似度
          var telSim =0.0
          if(telTel!=null&&tel!=null){
            telSim = TextSimilarity.similar(telTel,tel)
          }
          val totalSim = 0.9*nameSim + 0.05*addresSim+ 0.05*telSim

          (id,totalSim)
      }.filter(_._2>0.6).sortBy(k=>k._2).reverse.take(5)
      (selId,simiPois)
    }).flatMap(f=>f._2.map(k=>(f._1,k._1,k._2))).map(f=>Row(f._1,f._2,f._3))

    val structType = StructType(Array(StructField("id",StringType,true),StructField("simiId",StringType,true),StructField("simiValue",DoubleType,true)))

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()
    val reDataFrame = spark.createDataFrame(result,structType)

    //println("DataFrame的长度：-----------------------------------------------"+count)

    //将结果存入hive,需要先进行临时表创建
    val poiRecTemp = "poi_rec_temp"
    val poiRec = "poi_match"

    spark.sql("drop table if exists "+poiRec)
    spark.sql("create table if not exists " +poiRec+ "(id string,simiId string,simiValue double)")

    reDataFrame.createOrReplaceTempView(poiRecTemp)


    spark.sql("truncate table " + poiRec)
    spark.sql("insert into table " + poiRec + " select * from " + poiRecTemp)
  }
}
