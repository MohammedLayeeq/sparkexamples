package com.layeeq.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object WordCount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("WordCount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("WordCount").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    // val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val data = "C:\\work\\datasets\\dataset.txt"
    //val output ="C:\\work\\datasets\\output ;\\wordcount"
    val data = args(0)
    val output = args(1)
    val drdd = sc.textFile(data)
    val pro = drdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)
    pro.coalesce(1).saveAsTextFile(output)
    spark.stop()
  }
}