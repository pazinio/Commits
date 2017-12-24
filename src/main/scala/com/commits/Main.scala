package com.commits

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.time.LocalDate

case class NameByDate(date: java.sql.Date, name: String)
case class NameByDay(date: java.sql.Date, dateWithNoTime: java.sql.Date, dayOfWeek: String, name:String)
case class TotalBySpecificDate(date: java.sql.Date, dayOfWeek: String, total: Int)


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
    import org.apache.spark.sql.functions._

    /**
      * reading commits authors names per date (two column based csv)
      */
    val nameByDateDS = spark.read
      .option("header", "true")
      .csv("commits.csv")
      .as[NameByDate]

    /**
      * adding day of week and date without time for future data-set calculations
      */
    val nameByDayDS = nameByDateDS.map(r => new NameByDay(
        date = r.date,
        dateWithNoTime = ignoreTime(r.date),
        dayOfWeek = LocalDate.parse(r.date.toString).getDayOfWeek.toString,
        name = r.name
    ))


    /**
      * total per specific day (ignores HH:MM:SS)
      */
    val totalBySpecificDayDS = nameByDayDS.map(r => (r, 1))
      .groupByKey(_._1.dateWithNoTime)
      .reduceGroups((a,b) => (a._1, a._2 + b._2 ))
      .map(x => new TotalBySpecificDate(date = x._2._1.date, dayOfWeek = x._2._1.dayOfWeek,total = x._2._2))
    totalBySpecificDayDS.show(10)

    /**
      * now aggregate by total and find mean and standard deviation
      */
    spark.time {
      val averageByDay = totalBySpecificDayDS.groupBy($"dayOfWeek")
        .agg(
            avg($"total").as("avg"),
            stddev($"total").as("standard deviation")
          )
          .orderBy(org.apache.spark.sql.functions.col("avg").desc)
      averageByDay.show(7)

      // task 2 - anomalous days

      /**
        * build map from day to (avg, deviation)
        */
      val resMap =
        averageByDay.collect.map(r => r.get(0).toString -> (r.getDouble(1), r.getDouble(2))).toMap

      /**
        * Filter anomalies days
        */
      println("filter anomalies days")
      val anomaliesDays = totalBySpecificDayDS.filter(
        day => (day.total - resMap(day.dayOfWeek)._1) > 2.0 * resMap(day.dayOfWeek)._2
      )
      anomaliesDays.show(10)

      // task 3
      /**
        * Find the user with max commits of
        */
      println("max commits day from anomalies days")
      val maxCommitsDay = anomaliesDays.groupBy("date", "dayOfWeek").max("total").as("total")
      maxCommitsDay.show(1)

      nameByDayDS.join(maxCommitsDay, "date")
      .groupBy("name", "date", "dayOfWeek")
      .count
      .orderBy($"count".desc)
      .show(10)
    }

    /**
      * return 95 percentile via spark sql
      * https://stackoverflow.com/questions/41659695/sql-percentile-on-dataframe-with-float-numbers-spark-1-6-any-possible-workarou
      */
    spark.time {
      totalBySpecificDayDS.createOrReplaceTempView("df")
      val df = spark.sqlContext.sql(
        "select dayOfWeek, percentile(total,0.95) as 95h from df group by dayOfWeek"
      )
      df.show(7)
    }

    spark.stop()
  }

  private def ignoreTime(date: java.sql.Date): java.sql.Date = {
    val msSince1970 = date.getTime
    val withNoTime = msSince1970 - (msSince1970 % 1000 * 60 * 60 * 24)
    new java.sql.Date(withNoTime)
  }

}

