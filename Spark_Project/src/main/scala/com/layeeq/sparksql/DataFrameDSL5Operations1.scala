package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object DataFrameDSL5Operations1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DataFrameDSL5").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameDSL5").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("DataFrameDSL5").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("delimiter",";")load(data)
    df.printSchema()
    df.select("age").show()
    df.select("age","job").where($"job".contains("-")).show()
    df.where($"job".contains("-")).where($"loan" === "yes").show()
    df.where($"job".contains("-")).where(($"loan" === "yes") && ($"balance" >10000)).show()
    df.show(5)

    //show the result in Array From all this , they will use for testing perpose.
    df.first()
    df.take(8)
    df.head(4)
    df.collect()
    df.count()
    df.schema

    val data1 = "C:\\work\\datasets\\bank.csv"
    val df1 = spark.read.format("csv").option("inferSchema","true").option("delimiter",";").load(data1)

    //it will print with out schema column name.
    df1.printSchema()

    val rename = df1.withColumnRenamed("_c0","age").withColumnRenamed("_c1","something")
    rename.printSchema()
    df1.where("_c0>30").show(10)
    rename.where("age>30").show(10)






    spark.stop()
  }
}