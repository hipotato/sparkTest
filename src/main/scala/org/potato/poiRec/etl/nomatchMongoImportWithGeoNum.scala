package org.potato.poiRec.etl

import com.potato.poiMatch.dto.GetGeoNumRe
import com.potato.poiMatch.util.HttpRequestUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/** 从mongoDB中导入数据
  * Created by potato on 2018/5/7.
  */
object nomatchMongoImportWithGeoNum {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.appName("nomatchMongoImportWithGeoNum").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val inputUri="mongodb://iw:iw%402017hu04@192.168.50.168:27017/mga-prod.no_match"
    val load = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "32")).load()

    val originRdd = load.selectExpr( "_id.oid","name","location.coordinates[0]","location.coordinates[1]","address","tel[0]").toDF("id","name","lng","lat","address","tel").repartition(200)

    val tempRdd = originRdd.filter(_.get(2)!=null).filter(_.get(3)!=null).map(a=>{
      val lng = a.getDouble(2)
      val lat = a.getDouble(3)
      val re = HttpRequestUtil.GetGeoNum(lng,lat,18);
      var geoNum = -1L
      if(re.getServer_status==200){
        geoNum = re.getGeoNum
      }
      (a.getString(0),a.getString(1),lng,lat,a.getString(4),a.getString(5),geoNum)
    }).rdd

    val structType = StructType(Array(StructField("id",StringType,true),StructField("name",StringType,true),StructField("lng",DoubleType,true),
      StructField("lat",DoubleType,true),StructField("address",StringType,true),StructField("tel",StringType,true),
      StructField("geoNum",LongType,true)))

    val finalDf = spark.createDataFrame(tempRdd.map(a=>Row(a._1,a._2,a._3,a._4,a._5,a._6,a._7)),structType)


    //将结果存入hive,需要先进行临时表创建
    val poiTableTempName = "no_match_temp"
    val poiTableName = "no_match"


    spark.sql("drop table if exists "+poiTableName)
    spark.sql("create table if not exists "+poiTableName+"(id string,name string,lng double, lat double,address string,tel string,geoNum long)")

    finalDf.createOrReplaceTempView(poiTableTempName)
    spark.sql("truncate table " + poiTableName)

    spark.sql("insert into table " + poiTableName + " select * from " + poiTableTempName +" where lat is not null and lng is not null")

  }
}
