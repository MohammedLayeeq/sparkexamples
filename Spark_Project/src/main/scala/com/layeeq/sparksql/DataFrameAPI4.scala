package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object DataFrameAPI4 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DataFrameAPI").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameAPI").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("DataFrameAPI").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    /*val data1 = "C:\\work\\datasets\\Key_indicator_districtwise.csv"
    val df1 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data1)
    df1.createOrReplaceTempView("healthdata")
    val res1 = spark.sql("select * from healthdata limit 5")
    res1.show

    val data2 ="C:\\work\\datasets\\us-500.csv"
    val df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("delimiter",";").load(data2)
    df2.printSchema()
    df2.show(5)
    df2.createOrReplaceTempView("ustable")
    val res2 = spark.sql("select web from ustable limit 10")*/

    val data3 ="C:\\work\\datasets\\us500Comma\\us-500.csv"
    val output ="C:\\work\\datasets\\us500Comma\\output"
    val df3 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data3)
    df3.printSchema()
    df3.show(5)
    df3.createOrReplaceTempView("ustable")
    val res3 = spark.sql("select * from ustable limit 10")
    res3.write.format("csv").option("header","true").option("delimiter","|").save(output)

    spark.stop()
  }
}