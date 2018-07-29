package com.layeeq.sparkstreaming

import org.apache.spark._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object SparkStreamingWC {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkStreamingWC").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
   // val spark = SparkSession.builder.master("local[*]").appName("SparkStreamingWC").getOrCreate()
   // val sc = spark.sparkContext
   // val conf = new SparkConf().setAppName("SparkStreamingWC").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   // val sqlContext = spark.sqlContext
    //import spark.implicits._
   // import spark.sql

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10)) //every 10 sec create micro batch
    val lines = ssc.socketTextStream("ec2-13-126-147-88.ap-south-1.compute.amazonaws.com", 1112)// lines = Dstreme
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //spark.stop()
  }
}