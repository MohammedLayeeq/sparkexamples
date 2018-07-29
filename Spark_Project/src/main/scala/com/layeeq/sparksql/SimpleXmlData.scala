package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleXmlData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SimpleXmlData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SimpleXmlData").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("SimpleXmlData").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data1 = "C:\\work\\datasets\\xmldata\\xmldata.xml"
    val df1 = spark.read.format("com.databricks.spark.xml").option("rowTag","book").option("rootTag","catalog").load(data1)
    df1.printSchema()
    df1.show(100)
    spark.stop()
  }
}