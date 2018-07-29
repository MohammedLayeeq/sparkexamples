package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object  CaseClassDataFrame2 {
  case class Uscc(first_name:String, last_name:String, company_name:String, address:String, city:String, county:String, state:String,zip:Int, phone1:String, phone2:String, email:String, web:String)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("CaseClassDataFrame2").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("CaseClassDataFrame2").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("CaseClassDataFrame2").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data1 = "C:\\work\\datasets\\us-500.csv"
    val usrdd = sc.textFile(data1)
    val head1 = usrdd.first()
    val pro = usrdd.filter(x=>x!=head1).map(x=>x.split(";")).map(x=>Uscc(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9),x(10),x(11))).toDF()
    //pro.show(5)
    pro.createOrReplaceTempView("ustable")
    val usselectquery = spark.sql("select count(*) cnt from ustable")
    usselectquery.show()
    spark.stop()
  }
}