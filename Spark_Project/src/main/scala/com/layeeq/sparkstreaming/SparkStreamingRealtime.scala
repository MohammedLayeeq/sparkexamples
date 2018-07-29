package com.layeeq.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingRealtime {
  def main(args: Array[String]) {
  /*  //val spark = SparkSession.builder.master("local[*]").appName("SparkStreamingRealtime").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SparkStreamingRealtime").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("SparkStreamingRealtime").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    spark.stop()*/
    /*val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(30))

    windowedWordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()*/

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10)) //every 10 sec create micro batch
    val lines = ssc.socketTextStream("ec2-13-126-147-88.ap-south-1.compute.amazonaws.com", 2222)// lines = Dstreme

    lines.foreachRDD { x =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val df = x.map(t=>t.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()
      df.createOrReplaceTempView("logs")
      val logs1 = spark.sql("select name from logs where name='layeeq'")
      logs1.show()
      val url = "jdbc:oracle:thin://@oracledb.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val username = "ousername"
      val password = "opassword"

      //val tabs = Array("emp", "dept")
      //tabs.foreach { x =>
      val oprop = new java.util.Properties()
      oprop.setProperty("user", username)
      oprop.setProperty("password", password)
      oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
      logs1.write.mode(SaveMode.Append).jdbc(url,"layeeqlog",oprop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}