

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.7"//,
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-avro-reader",
    mainClass in Compile := Some("main.scala.GroupByCustomerGroup"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1",
    libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2",
    libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1",
    libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.6.3",
    libraryDependencies += "com.univocity" % "univocity-parsers" % "1.5.6",
    libraryDependencies += "org.kitesdk" % "kite-parent" % "1.1.0",
    libraryDependencies += "org.kitesdk" % "kite-data" % "1.1.0",
    libraryDependencies += "org.kitesdk" % "kite-data-core" % "1.1.0"
  )

