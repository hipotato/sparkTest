package org.potato.movie


import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.sql._
//import org.apache.phoenix.spark._
import org.potato.movie.caseClass._

object RecommendForAllUsers  {
  //在集群中提交这个main进行运行的时候，需要通过--jars来把mysql的驱动jar包所在的路径添加到classpath
  //请大家按照pom.xml中指定的版本，安装hbase1.2.6以及phoenix4.9
  //如果需要写入到Phoenix,则也需要添加一些相关的jar包添加到classpath
  //推荐大家通过maven assembly:assembly的方式打成jar包，然后在集群中运行
  def main(args: Array[String]) {

//    val spark=SparkSession.builder.master("yarn-cluster").appName("Train").enableHiveSupport().getOrCreate()
//    import spark.sql
//
//    val conf = new Configuration()
//    val users = sql("select distinct(userId) from trainingData order by userId asc")
//    val allusers = users.rdd.map(_.getInt(0)).toLocalIterator
//
//    //方法1，可行，但是效率不高
//    val modelpath = "/tmp/bestmodel/0.8215454233270015"
//    val model = MatrixFactorizationModel.load(spark.sparkContext, modelpath)
//    while (allusers.hasNext) {
//      val rec = model.recommendProducts(allusers.next(), 5)
//
//      writeRecResultToHbase(rec, conf, spark)
//    }
//
//    //把推荐结果写入到phoenix+hbase,通过DF操作，不推荐。
//    val hbaseConnectionString = "www-1:2181"
//    val userTupleRDD = users.rdd.map { x => Tuple3(x.getInt(0), x.getInt(1), x.getDouble(2)) }
//    userTupleRDD.saveToPhoenix("NGINXLOG_P", Seq("USERID", "MOVIEID", "RATING"), zkUrl = Some(hbaseConnectionString))
//
//    //把推荐结果写入到phoenix+hbase,通过DF操作，不推荐。
//    def writeRecResultToHbase(uid: Array[Rating],conf:Configuration, sparkSession:SparkSession) {
//      val uidString = uid.map(x => x.user.toString() + "|"
//        + x.product.toString() + "|" + x.rating.toString())
//      import sparkSession.implicits._
//      val uidDF = sparkSession.sparkContext.parallelize(uidString).map(_.split("|")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF()
//      uidDF.saveToPhoenix("movieRecommend",conf,Map("zkUrl" -> "www-1:2181").get("zkUrl"))
//      //大家按照自己的hbase配置的zookeeper的url来设置
//      //uidDF.toDF().saveToPhoenix("movieRecommend",conf,zkUrl)
//     // uidDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "phoenix_rec", "zkUrl" -> "localhost:2181"))
//    }
  }
}