package org.potato.sparksql

import org.apache.spark.sql.SparkSession

/**开窗函数
  * Created by potato on 2017/12/30.
  */
object RowNumberWindowFunction {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-client").appName("RowNumberWindowFunction").getOrCreate()
    import spark.sql
    import spark.implicits
    spark.sparkContext.parallelize("/data/sales.txt")
    spark.sql("drop table if exists sales")
    spark.sql("create table if not exists sales (product string,category string,revenue bigint)")
    spark.sql("load data inpath '/data/sales.txt' overwrite into table sales")
    val top3SalesDF =  spark.sql(""
      + "SELECT product,category,revenue "
      + "FROM ("
      + "SELECT "
      + "product,"
      + "category,"
      + "revenue,"
      // row_number()开窗函数的语法说明
      // 首先可以，在SELECT查询时，使用row_number()函数
      // 其次，row_number()函数后面先跟上OVER关键字
      // 然后括号中，是PARTITION BY，也就是说根据哪个字段进行分组
      // 其次是可以用ORDER BY进行组内排序
      // 然后row_number()就可以给每个组内的行，一个组内行号
      + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
      + "FROM sales "
      + ") tmp_sales "
      + "WHERE rank<=3")
    top3SalesDF.createOrReplaceTempView("top3_sales")
  }
}
