
import com.github.tototoshi.csv.{DefaultCSVFormat, CSVWriter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object Main extends App {

  val df = Spark.loadAvro("/data/test.avro")

  df.printSchema()

  val firstRow = df.limit(1).collect().head.toSeq.map(_.toString).mkString(",\t")

  Console.out.print(firstRow)

}