package org.potato.mobile

import com.potato.mobile.service.BehaviorStatService
import com.potato.utils.DateUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import net.liftweb.json._
/**
  * Created by potato on 2018/1/12.
  */
object UserBehaviorStreaming {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder.appName("UserBehaviorStreaming").getOrCreate()
    val checkPointPath = "hdfs:///data/checkpoint/mobile"

    val ssc = StreamingContext.getOrCreate(checkPointPath,createContext)
    ssc.start()
    ssc.awaitTermination()
  }
  case class SingleUserBehavior(packagename:String,activetime:Long)
  case class UserBehavor(begintime: Long,endtime: Long,userid: Long ,day: String,data:List[SingleUserBehavior])
  def createContext():StreamingContext ={
    val spark = SparkSession.builder.appName("UserBehaviorStreaming").getOrCreate()
    val groupId = "test-consumer-group"
    val broker = "search-1:9092,db-1:9092,www-1:9092"
    val kafkaParams = Map("bootstrap.servers" -> broker,"group.id"->groupId)
    val checkPointPath = "hdfs:///data/checkpoint/mobile"
    val topics = "movie".split(",").toSet
    //创建KafkaCluster对象
    val kafkaCluster = new KafkaCluster(kafkaParams)
    //获取topicAndPartitionSet对象
    val topicAndPartitionSet = kafkaCluster.getPartitions(topics).right.get
    var consumerOffsetsLong:Map[TopicAndPartition, Long] = Map()

    var A:Map[Char,Int] = Map()
    //当首次消费kafka的时候，zookeeper中没有记录offset，这时候将offset设置为0
    if(kafkaCluster.getConsumerOffsets(groupId,topicAndPartitionSet).isLeft) {
      consumerOffsetsLong = topicAndPartitionSet.map(a=>(a,0L)).toMap
    }else {
      consumerOffsetsLong = kafkaCluster.getConsumerOffsets(groupId,topicAndPartitionSet).right.get
    }

    //打印从zookeeper中获取的每个分区消费到的位置
    consumerOffsetsLong.foreach(print)
    //创建StreamingContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint(checkPointPath)

    def foo(a:MessageAndMetadata[String,String]) = a.message()
    //创建DStream
    val kafkaDirectStream =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,String](
      ssc,kafkaParams,
      consumerOffsetsLong,
      (a:MessageAndMetadata[String,String])=>a.message()
    )
    //将新的offset记录下来

    //println("Dstream数量："+kafkaDirectStream.count())
    //将json转换为UserBehavor，并筛选出符合条件的对象
    val filteredDStream = kafkaDirectStream.map(log=>{
     // val str = "{\"userid\":2000,\"day\":\"2017-02-15\",\"begintime\":1487116800000,\"endtime\":1487117400000,\"data\":[{\"packagename\":\"com.iwhere.footmark\",\"activetime\":60000},{\"packagename\":\"com.iwhere.footmark\",\"activetime\":60000}]}"
      //val userBehavor:UserBehavor = createObjectFromJson[UserBehavor](str)
      implicit val formats = DefaultFormats
     parse(log).extract[UserBehavor]
    }).filter(userBehavor=>(!(userBehavor==null||userBehavor.userid==0||userBehavor.data==null||userBehavor.data.size==0)))
    filteredDStream.print()

    //生成（UserHourPackageName，ActiveTime）类型的数据，并Reducebykey计算时间总和
    val UserHourPackageAndActiveTimeDstream = filteredDStream.flatMap(a=>{
     val  hour = DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT,a.begintime)
      a.data.map(b=>(a.userid+"|"+hour+"|"+b.packagename,b.activetime))
    }).reduceByKey(_+_)
    UserHourPackageAndActiveTimeDstream.print()
    //下一步，将数据写入hbase中
    UserHourPackageAndActiveTimeDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        val service = BehaviorStatService.getInstance(null)
        while (it.hasNext){
          val value = it.next()
          println("value="+value)
          service.addTimeLen(value._1.split("\\|")(0),value._1.split("\\|")(1),value._1.split("\\|")(2),value._2)
        }
      })
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsets.foreach(offset=>{
        val topicAndPartiton = TopicAndPartition(offset.topic,offset.partition)
        val topicAndPartitionMap = Map(topicAndPartiton->offset.untilOffset)
        kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionMap)
      })
    })

    //返回的对象
    ssc
  }

}
