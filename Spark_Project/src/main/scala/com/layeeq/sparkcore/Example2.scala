package com.layeeq.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example2 {
  //noinspection ComparingUnrelatedTypes
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Example2").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Example2").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Example2").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

   /* val data = "C:\\work\\datasets\\dataset.txt"
    val drdd = sc.textFile(data)
    val process = drdd.map(x=>x.split(" ")) */
     val data = "C:\\work\\datasets\\us-500.csv"
    val drdd = sc.textFile(data)
    val head = drdd.first()
    //noinspection ComparingUnrelatedTypes
    //val process = drdd.map(x=> x.split(",")).map(x => x(6)).distinct()
   // val process = drdd.filter(x => x!= head).map(x => x.split(",")).map(x => (x(0).replaceAll("\"", " "), x(1).replaceAll("\"", ""))).distinct()
    //val process = drdd.filter(x => x(6)!= "state" ).map(x => x.split(",")).map(x => (x(0).replaceAll("\"", " "), x(1).replaceAll("\"", ""))).distinct()
     val process = drdd.filter(x => x(1)!= "last_name" ).map(x => x.split(";")).map(x =>x(1).replaceAll("\"", "")).distinct()
    process.count
    process.take(20).foreach(println)
    //process.collect().foreach(println)


    spark.stop()
  }
}