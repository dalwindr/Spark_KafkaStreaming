//spark-shell --driver-class-path /Users/keeratjohar2305/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.1.1.jar:/Users/keeratjohar2305/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.11/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar:/usr/local/Cellar/external-jars/*
//https://gist.github.com/tsusanto/caaab4435a8e790a024542b9a8e45edc
//https://github.com/databricks/spark-xml/tree/master/src/main/scala/com/databricks/spark/xml
//https://sonra.io/2017/11/27/advanced-spark-structured-streaming-aggregations-joins-checkpointing/
/*

val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
// Split the lines into words
val words = lines.as[String].flatMap(_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()

val query = wordCounts.writeStream.outputMode("append").format("console").start()
query.stop



*/
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object generalXMLparser{
	
def main (args: Array[String]) {
	
val spark = SparkSession
 .builder
 .appName("Spark-Kafka-Integration")
 .master("local")
 .getOrCreate()
import spark.implicits._ // use along with sparkSession context		


var xmlFile="/Users/keeratjohar2305/Downloads/sampleXML.xml"
var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "Transaction").load(xmlFile)
val flattened = df.withColumn("LineItem", explode($"RetailTransaction.LineItem"))

val selectedData = flattened.select($"RetailStoreID",$"WorkstationID",$"OperatorID._OperatorName" as "OperatorName",$"OperatorID._VALUE" as "OperatorID",$"CurrencyCode",$"RetailTransaction.ReceiptDateTime",$"RetailTransaction.TransactionCount",$"LineItem.SequenceNumber",$"LineItem.Tax.TaxableAmount")
selectedData.show(3,false)

selectedData.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("POSLog-201409300635-21_lines")

}

}

