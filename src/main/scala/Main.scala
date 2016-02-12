
import java.io.FileInputStream
import java.util.Properties

import org.apache.avro.Schema.{Field, Parser}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.kitesdk.data._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Main extends App {


  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("config.properties"))

  // print properties
  Console.out.println("Properties:")
  properties.keysIterator.foreach(property => {
    Console.out.println(property + " = " + properties.get(property))
  })

  // load data
  val data = Spark.loadAvro(properties.get("fileLocation").toString)

  // count distinct values for attributes
  if (properties.get("mode").toString.contains("count")) {

    val tStart = java.util.Calendar.getInstance().getTimeInMillis

    val countAttributes: ListBuffer[String] = new ListBuffer[String]
    properties.getProperty("countDistinctValues").toString.split(",").foreach(x => countAttributes += x)

//    countAttributes.foreach(attribute => {
//      Console.out.println("\rDistinct values for " + attribute)
//      data.groupBy("type_name").count().show()
//      Console.out.println()
//    })

    Console.out.println("\n\rTime for counting: " + (java.util.Calendar.getInstance().getTimeInMillis - tStart) / 1000)

  }

  // count_occurrences operation
  if (properties.get("mode").toString.contains("count_occurrences")) {

    val tStart = java.util.Calendar.getInstance().getTimeInMillis

    val countAttributes: ListBuffer[String] = new ListBuffer[String]
    properties.getProperty("countOccurrences").toString.split(",").foreach(x => countAttributes += x)
    Console.out.println("Counting attributes:")
    Console.out.println(countAttributes.mkString(",\n "))
    Console.out.println()

    val indexAttributeMap = new mutable.HashMap[String, Int]()
    data.schema.fieldNames.foreach(field => indexAttributeMap.put(field, data.schema.fieldIndex(field)))
//
//    val f = spark.sql.functions.udf((s: String) => {
//      if (s != null && !s.equalsIgnoreCase("null")) 1
//      else 0
//    })
//
//    val countColumns = countAttributes.map(data(_))
//    val projecttedData = data.select(countColumns.toSeq: _*)
//
//    countAttributes.foreach(attribute => {
//
//      val singleAttributeCount = projecttedData.withColumn(attribute + "_occurence", f(projecttedData(attribute)))
//        .groupBy(attribute + "_occurence").sum(attribute + "_occurence")
//      val filteredSingleAttributeCount = singleAttributeCount.filter(singleAttributeCount(attribute + "_occurence") > 0)
//
//      if (filteredSingleAttributeCount.count() > 0)
//        Console.out.println("\r" + attribute + ": " + filteredSingleAttributeCount.head()(1))
//      else
//        Console.out.println("\r" + attribute + ": 0")
//
//    })
//
//    Console.out.println("\n\rTime for counting: " + (java.util.Calendar.getInstance().getTimeInMillis - tStart) / 1000)

    val tStart3 = java.util.Calendar.getInstance().getTimeInMillis

    val sum = data.map( row => {
      val countList = new ListBuffer[Long]
      countAttributes.foreach( attribute => {
        val value = row.getString(indexAttributeMap(attribute))
        if ( value != null && !value.equalsIgnoreCase("null") )
          countList += 1
        else
          countList += 0
      } )

      Row.fromSeq(countList.toSeq)

    }).reduce( (r1,r2) => {
      var sum : List[Long] = Nil

      for ( i <- 0 to r1.size - 1) {
        sum = r1.getLong(i) + r2.getLong(i) :: sum
      }

      Row.fromSeq(sum.toSeq)
    } )


    val attributeArray = countAttributes.toArray
    for ( i <- 0 to sum.size - 1) {
      Console.out.println("\r " + attributeArray(i) + ": " + sum.getLong(i) )
    }




    Console.out.println("Time for counting: " + (java.util.Calendar.getInstance().getTimeInMillis - tStart3) / 1000)

  }





  // filter
  if (properties.get("mode").toString.contains("filter")) {
    val filterAttribute = properties.get("filterAttribute")
    val filterValue = properties.get("filterValue")
    val filterOperator = properties.get("filterOperator")
    Console.out.println(filterAttribute + " " + filterOperator + " " + filterValue)
  }


}