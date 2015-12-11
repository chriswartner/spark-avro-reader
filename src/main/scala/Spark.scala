import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark {

  // Spark configuration
  private val conf = new SparkConf()
    .setAppName("diesunddas")
    .setMaster("local[*]")

  val context = new SparkContext(conf)
  val sql = new SQLContext(context)

  // Shutdown Spark
  def stop(): Unit = {
    context.stop()
  }


  def loadAvro(file: String) : DataFrame = {

    null
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
