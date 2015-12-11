

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.7"//,
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "diesunddas",
    mainClass in Compile := Some("main.scala.GroupByCustomerGroup"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1",
    libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
  )


