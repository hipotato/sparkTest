package org.potato.sparksql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**自定义函数
  * Created by potato on 2017/12/30.
  */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("DataFrameCreate").getOrCreate()
    val names = Array("potato","YangGH","Leo","you are my lover")
    val namesRDD = spark.sparkContext.parallelize(names,4)
    val namesRowRDD = namesRDD.map { name => Row(name) }
    val structType = StructType(Array(StructField("name", StringType, true)))

    val nameDF = spark.createDataFrame(namesRowRDD,structType)
    nameDF.createOrReplaceTempView("name")
    spark.udf.register("strLen",(str:String)=>str.length)
    spark.sql("select name,strLen(name) from name").show()
  }
}
