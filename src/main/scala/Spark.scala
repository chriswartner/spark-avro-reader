import java.io.{File, FileWriter}

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

object Spark {

  // Spark configuration
  private val conf = new SparkConf()
    .setAppName("spark-avro-reader")
    .setMaster("local[*]")

  val context = new SparkContext(conf)
  val sql = new SQLContext(context)

  // Shutdown Spark
  def stop(): Unit = {
    context.stop()
  }

  def writeCommaCSV(dataFrame: DataFrame, file: String) = {

    val settings = new CsvWriterSettings
    settings.setQuoteAllFields(true)

    val out = new FileWriter(new File(file))
    val writer = new CsvWriter(out, settings)
    val headers = dataFrame.columns.mkString(",")

    writer.writeRow(headers)
    dataFrame.collect().foreach(row =>
      writer.writeRow(row.mkString(","))
    )
    writer.close()
    out.close()

  }

  def loadAvro(file: String): DataFrame = {
//    sql.setConf("spark.sql.avro.compression.codec", "deflate")
//    sql.setConf("spark.sql.avro.deflate.level", "5")
    sql.read.avro(file)
  }

  def storeAvro(df: DataFrame, directory: String): Unit = {
    df.coalesce(1).write.format("com.databricks.spark.avro").save(directory)
  }

  def loadCsvSemi(file: String) = {
    Spark.sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(file)
  }

  def loadCsvComma(file: String) = {
    Spark.sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(file)
  }

  def loadCsvTab(file: String) = {
    Spark.sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load(file)
  }

}
