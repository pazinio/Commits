package com.commits

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.time.LocalDate


case class NameByDate(date: java.sql.Date, name: String)
case class NameByDay(date: java.sql.Date, dateWithNoTime: java.sql.Date, dayOfWeek: String, name:String)
case class TotalByDay(day: String, total:Int)


object Main {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Holds here to prevent NotSerializableException
//  val df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:MM:SS")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Commits")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._
    val sample = spark.createDataset(1 to 1000).reduce(_ + _)
    println(s"sample -> $sample")

    /**
      * reading commits authors names per date (two column based csv)
      */
    val nameByDateDS = spark.read
      .option("header", "true")
      .csv("commits.csv")
      .as[NameByDate]
    nameByDateDS.show(20)

    /**
      * adding day of week and date without time for future data set calculations
      */
    val nameByDayDS = nameByDateDS.map(r => new NameByDay(
        date = r.date,
        dateWithNoTime = ignoreTime(r.date),
        dayOfWeek = LocalDate.parse(r.date.toString).getDayOfWeek.toString,
        name = r.name
    ))
    nameByDayDS.show(20)


    /**
      * total per day of week - result logs
      */
    val totalByDayDS = nameByDayDS.limit(1000).map(r => (r.dayOfWeek, 1))
      .groupByKey(_._1)
      .reduceGroups((a,b) => (a._1, a._2 + b._2 ))
      .map(x => new TotalByDay(day = x._2._1, total = x._2._2))
    totalByDayDS.show()

    spark.stop()
  }

  private def ignoreTime(date: java.sql.Date): java.sql.Date = {
    val msSince1970 = date.getTime
    val withNoTime = msSince1970 - (msSince1970 % 1000 * 60 * 60 * 24)
    new java.sql.Date(withNoTime)
  }

}

