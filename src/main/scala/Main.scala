
import java.util.Properties

import org.apache.spark
import org.apache.spark.sql.Row

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
    var data = Spark.loadAvro(properties.get("fileLocation").toString)

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
        Console.out.println(" " + countAttributes.mkString(",\n "))
        Console.out.println()

        val indexAttributeMap = new mutable.HashMap[String, Int]()
        data.schema.fieldNames.foreach(field => indexAttributeMap.put(field, data.schema.fieldIndex(field)))

        val f = spark.sql.functions.udf((s: String) => {
            if (s != null && !s.equalsIgnoreCase("null")) 1
            else 0
        })

        val countColumns = countAttributes.map(data(_))
        val projectedData = data.select(countColumns.toSeq: _*)

        countAttributes.foreach(attribute => {

            val singleAttributeCount = projectedData.withColumn(attribute + "_occurence", f(projectedData(attribute)))
              .groupBy(attribute + "_occurence").sum(attribute + "_occurence")
            val filteredSingleAttributeCount = singleAttributeCount.filter(singleAttributeCount(attribute + "_occurence") > 0)

            if (filteredSingleAttributeCount.count() > 0)
                Console.out.println("\r" + attribute + ": " + filteredSingleAttributeCount.head()(1))
            else
                Console.out.println("\r" + attribute + ": 0")

        })

        Console.out.println("\n\rTime for counting: " + (java.util.Calendar.getInstance().getTimeInMillis - tStart) / 1000)
        Console.out.println()

        val tStart3 = java.util.Calendar.getInstance().getTimeInMillis

        val sum = data.map(row => {
            val countList = new ListBuffer[Long]
            countAttributes.foreach(attribute => {
                val value = row.getString(indexAttributeMap(attribute))
                if (value != null && !value.equalsIgnoreCase("null"))
                    countList += 1
                else
                    countList += 0
            })

            Row.fromSeq(countList.toSeq)

        }).reduce((r1, r2) => {
            var sum: List[Long] = Nil

            for (i <- 0 to r1.size - 1) {
                sum = r1.getLong(i) + r2.getLong(i) :: sum
            }

            Row.fromSeq(sum.toSeq)
        })

        var overallCount: Long = 0
        val attributeArray = countAttributes.toArray
        for (i <- 0 to sum.size - 1) {
            overallCount += sum.getLong(i)
            Console.out.println("\r " + attributeArray(i) + ": " + sum.getLong(i))
        }

        Console.out.println()
        Console.out.println(" Overall count: " + overallCount)
        Console.out.println()
        Console.out.println("Time for counting: " + (java.util.Calendar.getInstance().getTimeInMillis - tStart3) / 1000)
    }

    // filter
    if (properties.get("mode").toString.contains("filter")) {
        val filterAttribute = properties.get("filterAttribute").toString
        val filterValue = properties.get("filterValue").toString
        val filterOperator = properties.get("filterOperator").toString
        Console.out.println(filterAttribute + " " + filterOperator + " " + filterValue)
        Console.out.println()
        Console.out.println("\rSize before filtering: " + data.count())




        if (filterOperator.equalsIgnoreCase("not")) {

            if (filterValue.split(",").size == 1) {

                val fNot = (s: String) => {
                    if (s == null) true
                    else !s.contains(filterValue)
                }
                val fNotUdf = spark.sql.functions.udf(fNot)

                data = data.filter(fNotUdf(data(filterAttribute)))
            }

            else
                filterValue.split(",").foreach(attribute => {

                    val fNot = (s: String) => {
                        if (s == null) true
                        else !s.contains(attribute)
                    }
                    val fNotUdf = spark.sql.functions.udf(fNot)

                    data = data.filter(fNotUdf(data(filterAttribute)))

                })
        }

        Console.out.println("\rSize after filtering: " + data.count())

        Spark.storeAvro(data, properties.get("outputLocation").toString)

    }


}
