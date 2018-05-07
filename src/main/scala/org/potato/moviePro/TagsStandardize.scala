package org.potato.moviePro

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scala.util.control.Breaks
import com.hankcs.hanlp.HanLP

/**预处理评论，提取关键词并进行同义词合并，
  * 结果以（movieId，tag，count）的格式存入hive表中
  * Created by potato on 2018/5/5.
  */
object TagsStandardize {


  val tagsTableName = "tags"
  val tableName  = "tagsStandardizePro"
  val tagsStandardizeTempTable = "tagsStandardize_lite_temp"

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

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("tagsStandardize").enableHiveSupport().getOrCreate()//spark任务名称
    import spark.sql
    val tagsData = spark.sql("select movieId,tag from "+tagsTableName).rdd
    //标准化
    val tagsStandardize = tagsData.map(row=>{

      val tag = row.getString(1)
      val keywords = HanLP.extractKeyword(tag,1)

      (row.getInt(0),keywords)}
    ).filter(row=>{row._2!=null&&row._2.size()>0}).map(f=>{(f._1,f._2.get(0))})
    /**tag合并*/
    val tagsStandardizeTmp = tagsStandardize.collect()

    val broadcastValues = spark.sparkContext.broadcast(tagsStandardizeTmp)
    val tagsSimi = tagsStandardize.mapPartitions(part=>{

      //var result = List[Tuple2]()
      val myList = part.toList

      var re =myList.map(f=>{
        var retTag = f._2
        if (f._2.toString.split(" ").size == 1) {
          var simiTmp = ""
          val tagsTmpStand = broadcastValues.value
            .filter(_._2.toString.split(" ").size != 1 )
            .filter(f._2.toString.size < _._2.toString.size)
            .sortBy(_._2.toString.size)
          var x = 0
          val loop = new Breaks
          tagsTmpStand.map{
            tagTmp=>
              val flag = getEditSize(f._2.toString,tagTmp._2.toString)
              if (flag == 1){
                retTag = tagTmp._2
                loop.break()
              }
          }
          ((f._1,retTag),1)
        } else {
          ((f._1,f._2),1)
        }
      })
      re.iterator
    }).reduceByKey(_+_).map(a=>(a._1._1,a._1._2,a._2))


    /**将结果写入hive表中*/
    //我们先进行DataFrame格式化申明
    val structType = StructType(Array(StructField("movieId",IntegerType,true),StructField("tag",StringType,true),StructField("count",IntegerType,true)))

    val tagStandDataFrame = spark.createDataFrame(tagsSimi.map(a=>Row(a._1,a._2,a._3)),structType)

    //将结果存入hive,需要先进行临时表创建
    sql("drop table if exists "+tableName)
    sql("create table if not exists " +tableName+ "(movieId int,tag string,count int)")

    tagStandDataFrame.createOrReplaceTempView(tagsStandardizeTempTable)
    spark.sql("truncate table " + tableName)
    spark.sql("insert into table " + tableName + " select * from " + tagsStandardizeTempTable)
  }

}
