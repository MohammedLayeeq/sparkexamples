package com.layeeq.sparksql
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ComplexXMLData1 {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.layeeq.sparksql.ComplexXMLData1")
    //val spark = SparkSession.builder.master("local[*]").appName("ComplexXMLData1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ComplexXMLData1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ComplexXMLData1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data1 = "C:\\work\\datasets\\xmldata\\ravikirancomplex_trans00005.xml"
    val df1 = spark.read.format("com.databricks.spark.xml").option("rowTag","Transaction").option("rootTag","POSLog").load(data1)
    df1.printSchema()
    df1.show(100)
    df1.createOrReplaceTempView("table")
    val ndf1 = spark.sql("select BeginDateTime, BusinessDayDate, EndDateTime, ReceiptNumber from table ")
    ndf1.show()
    ndf1.printSchema()
    val ndf2 = spark.sql("select OperatorID._OperatorType OpenOperator, OperatorID._VALUE OpenValue from table")
      //, bu._TypeCode typecode, bu._VAULE value, CurrencyCode  from table lateral view explode(BusinessUnit.UnitID) x as bu")
    ndf2.show()
    ndf2.printSchema()
    val ndf3 = spark.sql("select  BeginDateTime, BusinessDayDate, EndDateTime, ReceiptNumber, " +
      "OperatorID._OperatorType optype, OperatorID._VALUE valevale, bu.UnitID._TypeCode tycode, " +
      "bu.UnitID._VALUE val, " +
      "rs.BeginDateTime BeginDateTimenew, rs.EndDateTime EndDateTimenew, rs.Sale.ActualSalesUnitPrice, " +
      "rs.Sale.Associate.AssociateID._OperatorType OperatorType, " +
      "rs.Sale.Associate.AssociateID._VALUE VALUEok, rs.Sale.Description, rs.Sale.ExtendedAmount, " +
      "rs.Sale.ItemNotOnFileFlag  " +
      "from table lateral view explode(BusinessUnit) x as bu lateral view explode(RetailTransaction.LineItem) y as rs ")
    ndf3.show()
    ndf3.printSchema()
    //val df3 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(ndf3)

    val url = "jdbc:oracle:thin://@oracledb.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val username = "ousername"
    val password = "opassword"

    val oprop = new java.util.Properties()
    oprop.setProperty("user", username)
    oprop.setProperty("password", password)
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
    ndf2.write.mode(SaveMode.Append).jdbc(url,"best7",oprop)
    ndf1.write.mode(SaveMode.Append).jdbc(url,"best5",oprop)

    //ndf3.write.mode(SaveMode.Append).option("header", "true").csv("C:\\work\\datasets\\xmldata\\output1")

    spark.stop()
  }
}