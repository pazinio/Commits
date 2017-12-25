package com.commits

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.time.LocalDate


case class NameByDate(date: String, name: String)
case class NameByDay(date: String, dateWithNoTime: String, dayOfWeek: String, name:String)
case class TotalBySpecificDate(dateWithNoTime: String, dayOfWeek: String, total: Int)


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

    val path = org.apache.spark.SparkFiles.get("commits.csv")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**
      * reading commits authors names per date (two column based csv)
      */
    val nameByDateDS = spark.read
      .option("header", "true")
      .csv(path)
      .as[NameByDate]

    /**
      * adding day of week and date without time for future data-set calculations
      */
    val nameByDayDS = nameByDateDS.map(r => new NameByDay(
        date = r.date,
        dateWithNoTime = r.date.split(" ")(0),
        dayOfWeek = LocalDate.parse(r.date.split(" ")(0)).getDayOfWeek.toString,
        name = r.name
    ))

    /**
      * total per specific day
      */
    val totalBySpecificDayDS = nameByDayDS.map(r => (r, 1))
      .groupByKey(_._1.dateWithNoTime)
      .reduceGroups((a,b) => (a._1, a._2 + b._2 ))
      .map(x => new TotalBySpecificDate(dateWithNoTime = x._2._1.dateWithNoTime, dayOfWeek = x._2._1.dayOfWeek,total = x._2._2))

    /**
      * now aggregate by total and find mean and standard deviation
      */
    spark.time {
      val averageByDay = totalBySpecificDayDS.groupBy($"dayOfWeek")
        .agg(
            avg($"total").as("avg"),
            stddev($"total").as("deviation")
          )
          .orderBy($"avg".desc)

      averageByDay.show

      // task 2 - anomalous days

      /**
        * Filter and print anomalies days
        */
      println("Anomalies days:")
      val anomaliesDays = totalBySpecificDayDS
        .join(averageByDay, "dayOfWeek")
        .where($"total" - ($"avg" + $"deviation") > $"deviation")

      anomaliesDays.show

      // task 3
      /**
        * Find the user with max commits of max day
        */
      println("Max commits day [from anomalies days]:")
      val maxCommitsDay = anomaliesDays.agg(max($"total").as("total")).join(anomaliesDays, "total")
      maxCommitsDay.show(1)

    println("top 10 users in max commits day:")
      nameByDayDS.join(maxCommitsDay, nameByDayDS.col("dateWithNoTime") === maxCommitsDay.col("dateWithNoTime"))
      .groupBy("name")
      .count
      .orderBy($"count".desc)
      .show(10)
    }

    /**
      * return 95 percentile via spark sql
      * https://stackoverflow.com/questions/41659695/sql-percentile-on-dataframe-with-float-numbers-spark-1-6-any-possible-workarou
      */
    println("95 percentile:")
    spark.time {
      totalBySpecificDayDS.createOrReplaceTempView("df")
      val df = spark.sqlContext.sql(
        "select dayOfWeek, percentile(total,0.95) as 95th from df group by dayOfWeek"
      )
      df.show
    }

    spark.stop()
  }
}

