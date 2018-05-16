package org.potato.poiRec.etl

import org.apache.spark.sql.SparkSession

/** 从mongoDB中导入数据
  * Created by potato on 2018/5/7.
  */
object nomatchMongoImport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MongoSparkConnectorIntro").enableHiveSupport().getOrCreate()
    val inputUri="mongodb://iw:iw%402017hu04@192.168.50.168:27017/mga-prod.no_match"
    val load = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "32")).load()

    val originDf = load.selectExpr( "_id","name","location.coordinates[0]","location.coordinates[1]","address","tel[0]").toDF("id","name","lng","lat","address","tel")

    //将结果存入hive,需要先进行临时表创建
    val poiTableTempName = "no_match_temp"
    val poiTableName = "no_match"

    spark.sql("drop table if exists "+poiTableName)
    spark.sql("create table if not exists "+poiTableName+"(id string,name string,lng string, lat string,address string,tel string)")
    originDf.createOrReplaceTempView(poiTableTempName)
    spark.sql("truncate table " + poiTableName)
    spark.sql("insert into table " + poiTableName + " select * from " + poiTableTempName +" where lat is not null and lng is not null")
  }
}
