package com.layeeq.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example3 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Example3").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Example3").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Example3").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\datasets\\us-500.csv"
    val drdd = sc.textFile(data)
    val head = drdd.first()
    val process = drdd.filter(x => x != head).map(x => x.split(",")).map(x => (x(0).replaceAll("\"", ""), x(1).replaceAll("\"", ""))).
      filter( x => x._1.length<=4)
    //process.take(10).foreach(println)
    process.count

    spark.stop()
  }
}