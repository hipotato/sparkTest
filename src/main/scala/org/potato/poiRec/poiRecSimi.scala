package org.potato.poiRec

import com.potato.moviePro.{GeoNumUtil, TextSimilarity}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


/** 相似度计算
  * Created by potato on 2018/5/7.
  */
object poiRecSimi {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("poi_recommend_similar").enableHiveSupport().getOrCreate()//spark任务名称


    case class Poi(id:String,name:String,geo_num:Long,typecode:String,lat:String,lng:String)

    val poiRDD: RDD[Poi] = spark.sql("select id, name,geo_num,typecode,lat,lng from poi_zh limit 20000").rdd.map {
      f => Poi(f.getString(0),f.getString(1), f.getLong(2), f.getString(3), f.getString(4),f.getString(5))
    }.filter(_.geo_num!=null).filter(_.lat!=null).filter(_.lng!=null).filter(_.name!=null)
    /**从Hive中读取poi数据，形成DataFrame*/
    //从hive中，获取rating评分数据集，最终形成如下格式数据(movie,avg_rate)

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()
    val poiDf = poiRDD


    val poiColleted = poiDf.collect()
    case class ToSave(id:String,poi:Poi,simi:Double)
    val result = poiDf.map(a =>{

      val selId =  a.id
      val selName = a.name
      val selGeoNum = a.geo_num
      val selTypecode = a.typecode
      val selLng = a.lng
      val selLat = a.lat


      val simiPois  = poiColleted.map{
        ff=>
          val id =  ff.id
          val name = ff.name
          val geoNum = ff.geo_num
          val typecode = ff.typecode
          val lng = ff.lng
          val lat = ff.lat

          //名称相似度
          val nameSim = TextSimilarity.similar(selName,name)

          //网格码相似度
          val geoSim = GeoNumUtil.GeoNumSameByte(selGeoNum,geoNum)/64.0

          //类型相似度
          var typeSim =0.0;
          if(typecode.compare(selTypecode)==0){
            typeSim =1
          }else if(typecode.substring(0,4).compareTo(selTypecode.substring(0,4))==0){
            typeSim =0.7
          }else if(typecode.substring(0,2).compareTo(selTypecode.substring(0,2))==0){
            typeSim =0.3
          }

          val totalSim = 0.2*typeSim + 0.2*nameSim+ 0.6*geoSim

          (ff.id,totalSim)
      }.toList.sortBy(k=>k._2).reverse.take(20)

      (selId,simiPois)
    }).flatMap(f=>f._2.map(k=>(f._1,k._1,k._2))).map(f=>Row(f._1,f._2,f._3))

    val structType = StructType(Array(StructField("id",StringType,true),StructField("simiId",StringType,true),StructField("simiValue",DoubleType,true)))

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()
    val reDataFrame = spark.createDataFrame(result,structType)

    //println("DataFrame的长度：-----------------------------------------------"+count)

    //将结果存入hive,需要先进行临时表创建
    val poiRecTemp = "poi_rec_temp"
    val poiRec = "poi_rec"

    spark.sql("drop table if exists "+poiRec)
    spark.sql("create table if not exists " +poiRec+ "(id string,simiId string,simiValue double)")

    reDataFrame.createOrReplaceTempView(poiRecTemp)
    //spark.sql("truncate table " + poiRec)
    spark.sql("insert into table " + poiRec + " select * from " + poiRecTemp)
  }
}
