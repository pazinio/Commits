# Commits
Public github repositories commits statistics and anomalous days detections. 
(using Spark/Scala)

## Prerequisites:
1. spark 2.2.0 and above.
2. sbt (scala build tool)

## Instructions:
1. git clone [this repository]
2. sbt clean pacakge
3. spark-submit --files [commits.csv] [sbt-fullpath-output.jar]

## Clarifications & Basic Pipeline:
in spite of the fact that this assignment could be solved in more concise way,
I chose the type-safe way with scala Dataset and case classes to demonstrate typesafe workspace.

## Commits.csv (input-file):
in a real cluster environmet (standalone, Mesos or Yarn) in order to boost performance file(s) should be located under a distributed file system (HDFS/s3/etc)

you can find raw data under:
https://drive.google.com/open?id=1dsLhVGFA1n-_Yl5xd-NjhZyGB_MqIxwZ
