package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}


object ComplexJsonData2 {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.layeeq.sparksql.ComplexJsonData2")
    //val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData2").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData2").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ComplexJsonData2").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql


    val data = "C:\\work\\datasets\\JsonData\\companies.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("tab")
    val df1 = spark.sql("select * from  tab")
    df1.show(5)
    val df2 = spark.sql("select _id.`$oid` idoid, " +
      "acquisition.acquired_day acquired_day, acquisition.acquiring_company.name compname, " +
      "acquisition.acquiring_company.permalink comppermalink from tab ")
    df2.show()
    df2.printSchema()
    val df3 = spark.sql("select _id.`$oid` idoid, a.company.name accqcomname, " +
      "acquisition.acquired_day acquired_day," +
      " acquisition.acquiring_company.name compname," +
      " acquisition.acquiring_company.permalink comppermalink,f.id fundid, " +
      "f.investments investments from " +
      "tab lateral view explode(acquisitions) t as a lateral view explode(funding_rounds) t as f  ")
    df3.show()
    df3.printSchema()
    df3.createOrReplaceTempView("tab1")
    val df4 = spark.sql("select idoid, i.company.name icomname , i.person.first_name from tab1 " +
      "lateral view explode(investments) a as i")
    df4.show()
    val df5= spark.sql("select idoid,i.company.name icompname, i.company.permalink compperm, " +
      "i.financial_org.name ifinname, i.person.first_name from tab1 lateral view explode(investments) t as i")

    df5.show()

    spark.stop()
  }
}