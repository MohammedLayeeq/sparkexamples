package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object ProgramSchemaDataFrame3 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ProgramSchemaDataFrame3").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ProgramSchemaDataFrame3").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ProgramSchemaDataFrame3").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val rdddata = spark.sparkContext.textFile("C:\\work\\datasets\\us-500.csv")
    val head = rdddata.first()
    val schemaString = "first_name;last_name;company_name;address;city;county;state;zip;phone1;phone2;email;web"

    val fields = schemaString.split(";").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //Row acts as a case class
    val rowRDD = rdddata.filter(x=>x!=head).map(_.split(";")).map(x => Row(x(0), x(1),x(2),x(3), x(4),x(5),x(6), x(7),x(8),x(9), x(10),x(11)))

    val DF = spark.createDataFrame(rowRDD, schema)

    DF.createOrReplaceTempView("ustable")

    val results = spark.sql("SELECT * FROM  ustable")
    results.show(10)

    spark.stop()
  }
}