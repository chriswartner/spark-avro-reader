
import java.io.FileInputStream

import com.github.tototoshi.csv.{DefaultCSVFormat, CSVWriter}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, FloatType, IntegerType}
import org.kitesdk.data._
import java.io.FileInputStream
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, FileSystem, Path}
import org.kitesdk.data.spi.filesystem.InputFormatReader
import scala.compat.Platform
import scala.util.Random

object Main extends App {

  val attributes = "" :: ""
  
  val attribute = ""
  
  val schemaLocation = ""
  
  val dataLocation = ""

  val data = Spark.loadAvro(dataLocation)

  val schema = new Parser().parse(new FileInputStream(schemaLocation))

  val descriptor = new DatasetDescriptor.Builder().schema(schema).build()

  val records = Datasets.create("dataset:file:" + dataLocation, descriptor).asInstanceOf[View[GenericRecord]]

  val fileSystem = new LocalFileSystem()

  data.printSchema()

  val reader = records.newReader() //.asInstanceOf[DatasetWriter[GenericRecord]]

  var counter = 0
  while (reader.hasNext ) {
    val record = reader.next

    if (attributes.contains(record.get(attribute).toString) ) {
      counter += 1
    }
  }

  reader.close()

  Console.out.println("Record count: " + counter)

