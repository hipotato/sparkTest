package org.potato.movie


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation._
/**
  * Created by potato on 2017/12/23.
  */
object Train {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.master("yarn-cluster").appName("Train").enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("/data/checkPoint")
    import spark.sql

    val traningRdd = sql("select userid,movieid,rating from traningdata").rdd.map(a => Rating(a.getInt(0), a.getInt(1), a.getDouble(2).toFloat))
    val testRdd = sql("select userid,movieid,rating from testdata").rdd.map(a => Rating(a.getInt(0), a.getInt(1), a.getDouble(2).toFloat))

    val traning2 = traningRdd.map{
      case Rating(userId,movieId,rating)=>((userId,movieId),rating)
    }

    val test2 = testRdd.map{
      case Rating(userId,movieId,rating)=>((userId,movieId),rating)
    }

    val predictRdd = traningRdd.map{
      case Rating(userId,movieId,rating)=>(userId,movieId)
    }

    test2.persist()
    predictRdd.persist()
//    traningRdd.checkpoint()
//    test2.persist()
//    predictRdd.checkpoint()

    //最佳RMES初始化
    var bestRMES = Double.MaxValue
    var bestLambda =0.0
    var bestIteration = 0

    //特征向量个数
    val rank = 5
    //lambda因子
    val lambda =List(0.001,0.005,0.01,0.015,0.02,0.1)
    //迭代系数
    val iteration=List(10,20,30,40)
    //两层循环遍历lambda 和 iteration
    for(l<-lambda;i<-iteration){
      //训练当前lambda和iteration情况下的模型
      val model = ALS.train(traningRdd,rank,i,l)
      //用训练好的模型去预测测试数据的值
     val predict =  model.predict(predictRdd).map{
        case Rating(userId,movieId,rating)=>((userId,movieId),rating)
      }
      //对预测数据和真实数据进行join
      val predictAndFact = predict.join(test2)
      //计算模型偏差MES值
      val MES = predictAndFact.map{
        case((userId,movieId),(r1,r2))=>
          val err = r1 - r2
          err * err
      }.mean()
      val RMES = math.sqrt(MES)

      if(RMES<bestRMES){
        bestRMES = RMES
        bestLambda=l
        bestIteration=i
        model.save(spark.sparkContext,s"/data/BestModle/${bestRMES}")
      }
    }

    println(s"best model located in:/data/BestModle/${bestRMES}")
    println(s"best Lambda is ${bestLambda}")
    println(s"best Iteration is ${bestIteration}")
  }
}
