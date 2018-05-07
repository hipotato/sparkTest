//package org.potato.hbase
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
//import org.apache.hadoop.mapred.JobConf
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.rdd.RDD
//
///**
//  * Created by potato on 2017/12/19.
//  */
//object HbaseTableCopy {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("HbaseOperator").setMaster("yarn-client")
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.quorum", "www-1:2181,search-1:2181,db-1:2181")
//    hbaseConf.set("hbase.client.scanner.timeout.period", "6000")
//    hbaseConf.set("hbase.rpc.timeout", "60000")
//    hbaseConf.set("mapreduce.task.timeout", "12000")
//    hbaseConf.set("zookeeper.znode.parent", "/hbase")
//
//
//    val sc = new SparkContext(conf)
//    val jobConf = new JobConf(hbaseConf)
//
//    //val job = new Job()
//
//    //jobConf.setOutputFormat(TableOutputFormat[ImmutableBytesWritable])
//    //jobConf.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable
//      jobConf.setOutputFormat(TableOutputFormat[ImmutableBytesWritable,Result].getClass)
////    jobConf.setOutputFormat(classOf[TableOutputFormat[_,_]])
//
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, args(1))
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, args(0))
//    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, args(1))
//
//    val d: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//    val oldTableRdd = d
//    //println("rddCount="+oldTableRdd.count())
//    putHbase(oldTableRdd,jobConf)
//    sc.stop()
//  }
//  def putHbase(input:RDD[(ImmutableBytesWritable, Result)],jobConf:JobConf ): Unit ={
//
//    val newRdd = input.map(a=>{
//      val put = new Put(a._2.getRow)
//      val rawCell = a._2.rawCells()
//      rawCell.foreach(cell => put.add(cell))
//      (new ImmutableBytesWritable, put)
//    })
//
//    newRdd.saveAsNewAPIHadoopDataset(jobConf)
//  }
//}
