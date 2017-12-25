# Commits
spark public github repositories commits statistics

prerequisites:
spark 2.0 and above.

instructions:
1. git clone [this repository]
2. sbt clean pacakge
3. spark-submit sbt-fullpath-output.jar

Output Results:

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
+---------+------------------+------------------+                               
|dayOfWeek|               avg|         deviation|
+---------+------------------+------------------+
|  TUESDAY|130394.63461538461|15627.058249412878|
|WEDNESDAY|128284.26923076923|  10618.8665380795|
| THURSDAY|126155.21153846153| 9942.588543954438|
|   MONDAY|123075.03846153847|17609.502370140443|
|   FRIDAY|116852.43396226416|13126.508201271778|
|   SUNDAY| 81847.61538461539| 9909.358601261036|
| SATURDAY| 77923.15094339622| 8831.441589780288|
+---------+------------------+------------------+

Anomalies days:
+---------+----------+------+------------------+------------------+             
|dayOfWeek|      date| total|               avg|         deviation|
+---------+----------+------+------------------+------------------+
| SATURDAY|2016-01-30| 97208| 77923.15094339622| 8831.441589780288|
| SATURDAY|2016-02-20| 98057| 77923.15094339622| 8831.441589780288|
|  TUESDAY|2016-09-20|214123|130394.63461538461|15627.058249412878|
|   MONDAY|2016-09-19|209868|123075.03846153847|17609.502370140443|
|   SUNDAY|2016-02-21|102639| 81847.61538461539| 9909.358601261036|
+---------+----------+------+------------------+------------------+

Max commits day [from anomalies days]:
+------+---------+----------+------------------+------------------+             
| total|dayOfWeek|      date|               avg|         deviation|
+------+---------+----------+------------------+------------------+
|214123|  TUESDAY|2016-09-20|130394.63461538461|15627.058249412878|
+------+---------+----------+------------------+------------------+

+-----------------+-----+                                                       
|             name|count|
+-----------------+-----+
|          Emile B|93551|
|           pyjobs|  562|
|         nathanbl|  539|
|       miatribepi|  472|
|     Ryan Niehaus|  417|
|          Jenkins|  284|
|   Wassim Chegham|  273|
|           dwulff|  193|
|    truthlighting|  175|
|       Terry Yuan|  169|
|greenkeeperio-bot|  152|
|  morganseangibbs|  150|
|     sqrtofsaturn|  139|
|     Alan Knowles|  127|
|             keum|  125|
|            crazy|  123|
|Dominik Wilkowski|  115|
|     Evan Maloney|  114|
|             root|  112|
|         Shinmera|  110|
+-----------------+-----+
only showing top 20 rows

Time taken: 233416 ms
+---------+------------------+                                                  
|dayOfWeek|               95h|
+---------+------------------+
| SATURDAY|           91797.8|
| THURSDAY|          140693.2|
|   FRIDAY|133673.19999999998|
|  TUESDAY|         142191.45|
|WEDNESDAY|         142493.35|
|   MONDAY|139528.24999999997|
|   SUNDAY|          96530.75|
+---------+------------------+

Time taken: 44127 ms
