package com.layeeq.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example4 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Example4").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Example4").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Example4").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\dataset.txt"
    val output = "C:\\work\\datasets\\wordcount1"
    val drdd = sc.textFile(data)
   // val pro = drdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2, false)
    val pro = drdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._1, false)
    pro.take(20 ).foreach(println)
    pro.saveAsTextFile(output)
    spark.stop()
  }
}