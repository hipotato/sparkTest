package org.potato.poiRec

import com.potato.moviePro.{GeoNumUtil, TextSimilarity}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import spire.implicits


/** 相似度计算
  * Created by potato on 2018/5/7.
  */
object poiRecSimi2 {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("poi_recommend_similar").enableHiveSupport().getOrCreate()//spark任务名称
    spark.conf.set("spark.sql.shuffle.partitions","400")
    import spark.implicits._
    case class Poi(id:String,name:String,geo_num:Long,typecode:String,lat:String,lng:String)

//    val poiRDD: RDD[Poi] = spark.sql("select id, name,geo_num,typecode,lat,lng from poi_zh limit 20000").rdd.map {
//      f => Poi(f.getString(0),f.getString(1), f.getLong(2), f.getString(3), f.getString(4),f.getString(5))
//    }.filter(_.geo_num!=null).filter(_.lat!=null).filter(_.lng!=null).filter(_.name!=null)
    /**从Hive中读取poi数据，形成DataFrame*/
    //从hive中，获取rating评分数据集，最终形成如下格式数据(movie,avg_rate)

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()

    var selectPoi = "select id, name,geo_num,typecode,typecodeA from poi_zh limit 10000"

    val poiDf = spark.sql(selectPoi)

    val poiDf2 = poiDf

    val joined = poiDf.join(poiDf2,poiDf("typecodeA")===poiDf2("typecodeA"))

    val news = joined.map(row=>{
      val selId =  row.getString(0)
      val selName = row.getString(1)
      val selGeoNum = row.getLong(2)
      val selTypecode = row.getString(3)
      //val selLng = row.getDouble(4)
      //val selLat = row.getDouble(5)
      val otherId =  row.getString(5)
      val otherName = row.getString(6)
      val otherGeoNum = row.getLong(7)
      val otherTypecode = row.getString(8)
      //val otherLng = row.getDouble(10)
      //val otherLat = row.getDouble(11)

      //名称相似度
      val nameSim = TextSimilarity.similar(selName,otherName)
      //网格码相似度
      val geoSim = GeoNumUtil.GeoNumSameByte(selGeoNum,otherGeoNum)/64.0

      //类型相似度
      var typeSim =0.0;
      if(otherTypecode.compare(selTypecode)==0){
        typeSim =1
      }else if(otherTypecode.substring(0,4).compareTo(selTypecode.substring(0,4))==0){
        typeSim =0.7
      }else if(otherTypecode.substring(0,2).compareTo(selTypecode.substring(0,2))==0){
        typeSim =0.3
      }

      val totalSim = 0.2*typeSim + 0.2*nameSim+0.6*geoSim
      (selId,(otherId,totalSim))
    }).rdd.groupByKey().mapValues(v=>v.toList.sortBy(a=>a._2).reverse.take(21)).flatMap(ddd=>{
      val re2 = ddd._2.map(db=>(ddd._1,db._1,db._2)).filter(_._3<1.0)
      re2
    }).map(rr=>Row(rr._1,rr._2,rr._3))


    val structType = StructType(Array(StructField("id",StringType,true),StructField("simiId",StringType,true),StructField("simiValue",DoubleType,true)))

    //val df = movieContentBase.map(a=>Recs(a._1,a._2,a._3)).toDF()
    val reDataFrame = spark.createDataFrame(news,structType)

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
