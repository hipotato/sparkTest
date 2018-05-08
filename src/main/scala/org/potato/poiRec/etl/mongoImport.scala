package org.potato.poiRec.etl
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config._

/** 从mongoDB中导入数据
  * Created by potato on 2018/5/7.
  */
object mongoImport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro").getOrCreate()

    val inputUri="mongodb://iw:iw%402017hu04@192.168.50.168:27017/mga-prod.poi_zh"
    val load = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "32")).load()

    val poiRdd = load.rdd


    val originDf = load.selectExpr("_id", "name","geo_num", "type","typecode[0]","location.coordinates[0]","location.coordinates[1]").toDF("id", "name","geo_num", "type","typecode","lat","lng")



    //将结果存入hive,需要先进行临时表创建
    val poiTableTempName = "poi_zh_temp"
    val poiTableName = "poi_zh"

    spark.sql("drop table if exists "+poiTableName)
    spark.sql("create table if not exists "+poiTableName+"(id string,name string,geo_num long,type string,typecode string, lat string,lng string)")



    originDf.createOrReplaceTempView(poiTableTempName)
    spark.sql("truncate table " + poiTableName)

    spark.sql("insert into table " + poiTableName + " select * from " + poiTableTempName)



  }
}
