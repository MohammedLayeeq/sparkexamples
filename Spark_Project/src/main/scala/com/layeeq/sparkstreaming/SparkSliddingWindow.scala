package com.layeeq.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object SparkSliddingWindow {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkSliddingWindow").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    /*val spark = SparkSession.builder.master("local[*]").appName("SparkSliddingWindow").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("SparkSliddingWindow").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
*/
  //  spark.stop()

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10)) //every 10 sec create micro batch
    val lines = ssc.socketTextStream("ec2-13-126-147-88.ap-south-1.compute.amazonaws.com", 1338)// lines = Dstreme
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(30))

    windowedWordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}