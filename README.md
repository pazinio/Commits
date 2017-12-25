# Commits
spark public github repositories commits statistics

Prerequisites:
1. spark 2.2.0 and above.
2. sbt (scala build tool)

Instructions:
1. git clone [this repository]
2. sbt clean pacakge
3. spark-submit --files [commits.csv] [sbt-fullpath-output.jar]

Clarifications & Basic Pipeline:
in spite of the fact that this assignment could be solved in more concise way,
i chose the type-safe way with scala Dataset and case classes to demonstrate typesafe workspace.

