
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



  val data = Spark.loadAvro("/data/data.avro")

  data.printSchema()



  val schema = new Parser().parse(new FileInputStream("/data/schema.avsc"))

  val descriptor = new DatasetDescriptor.Builder().schema(schema).build()

  val records = Datasets.create("dataset:file:/data/v1", descriptor).asInstanceOf[View[GenericRecord]]

  val path = new Path("/data","v1")

  val fileSystem = new LocalFileSystem()

  val reader = records.newReader() //.asInstanceOf[DatasetWriter[GenericRecord]]

  var counter = 0
  while (reader.hasNext ) {
    val record = reader.next

//    val fields : Array[Schema.Field] = schema.getFields.toArray.map( _.asInstanceOf[Schema.Field])
//
//    fields.foreach(field => Console.out.println(field.name().toString + "\t"))

    if (record.get("type_name").equals("AppUsage") || record.get("type_name").equals("WebUsage") ) {
      counter += 1
    }
  }

  reader.close()

  Console.out.println("Record count: " + counter)




//
//  var data = Spark.loadCsvComma("/data/geoknow/order.csv")
//  var dataNew : DataFrame = _
//  var dataOut : DataFrame = data
//
//  data.show()
//
//  data = data.withColumn( "orderDate", data("orderDate").cast(StringType))
//
//  Console.out.println(data.count())
//
//  for( year <- 2000 until 2013){
//    val udfReplaceYear = udf((x: String) => {x.replace( 2014.toString,year.toString)})
//    val udfAppendYear = udf((x: String) => { x + year.toString})
//    dataNew = data.withColumn("orderDate", udfReplaceYear( data("orderDate")) )
//      .withColumn("order", udfAppendYear(data("order")) )
//    dataOut = dataOut.unionAll(dataNew)
//  }
//
//
//  dataOut = dataOut.dropDuplicates()
//
//  Spark.writeCommaCSV(dataOut, "/data/geoknow/order_new.csv")


}