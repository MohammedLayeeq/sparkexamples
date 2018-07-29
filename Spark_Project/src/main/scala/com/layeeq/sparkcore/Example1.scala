package com.layeeq.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Example").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Example").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Example").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val nums = Array(1,2,3,4,5,6,7,8,9,10)

    val numrdd = sc.parallelize(nums)
    val process = numrdd.map(x => x * x).filter(x=> x < 100).map(x=> x+x)
    process.collect().foreach(println)


    spark.stop()
  }
}