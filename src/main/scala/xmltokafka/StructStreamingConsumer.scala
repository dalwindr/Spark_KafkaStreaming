//spark-submit --class StructStreamingConsumer --master local[*] --driver-class-path /Users/keeratjohar2305/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.1.1.jar:/Users/keeratjohar2305/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.11/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar /Users/keeratjohar2305/Downloads/SPARK_POC/target/scala-2.11/spark_poc_2.11-0.1.jar

/*

DStream uses the times when the data arrives to spark
sparkStreaming(batch interval) , window(WindowLength,slidingIngterval) , countByWindow,ReduceByWindow,countByKeyAndWindow, ReduceByKeyAndWindow
Pain Points
1) 
  You want to process the data in the event in itself and tough to deal with late arriving data
2) DStream is for Streaming and RDD is for batches , they API are simlilar for RDD we need tranformation . more work for developer

3) there are challenge we have to code at various level to use difference kind of API/trnaformationa nd exeception handnling and So end to end accuracy is 
difficult to achieve


//df.select($"Key").writeStream.outputMode("complete").format("console").start()

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._



s2.stop()


var s =df.select($"Key",$"value",$"topic",$"partition",$"offset",$"timestamp",$"timestampType").writeStream.option("failOnDataLoss", false).format("console").start()

val s = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.option("failOnDataLoss", false).format("console").start()


.queryName("kafka2console-microbatch")

.withColumn("value", $"value" cast "string")


 /////////////////////
com.databricks.spark.xml
 ///////////////////

val inputPath = s"file://${homeDir}/test/resources/documents/.xml"


val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "dc:Document")
  .schema(schema)
  .load(inputPath)

df.show(10)


*/

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

object StructStreamingConsumer{
	
	def main(args: Array[String]) {


val spark = SparkSession
 .builder
 .appName("Spark-Kafka-Integration")
 .master("local")
 .getOrCreate()
import spark.implicits._ // use along with sparkSession context		


def xmlParser = udf { (stg: String) => {   (scala.xml.XML.loadString(stg) \\ "name").text} }

val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "demo_test").load()
/*
scala> df.printSchema
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
*/

var df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as xml").withColumn("newcol" ,xmlParser($"xml") ).select("newcol")
var df11 = df.selectExpr("CAST(value AS STRING) as xml").writeStream
//val ss = df1.writeStream.option("failOnDataLoss", false).option("startingOffsets","earliest").format("console").queryName("kafka2console-microbatch").start()
s.stop

//Micro-Batch Stream Processing( Trigger.Once and Trigger.ProcessingTime triggers)

var s = spark.sql("select * from TEST").writeStream.option("failOnDataLoss", false).format("console").queryName("kafka2console-microbatch").trigger(Trigger.ProcessingTime(30.seconds)).start()


//val s1 = df1.option("failOnDataLoss", false).format("console").queryName("kafka2console-microbatch").trigger(Trigger.ProcessingTime(30.seconds)).start()



//Continuous Stream Processing (Trigger.Continuous trigger) via KafkaContinuousReader)
//val s2 = df1.writeStream.option("failOnDataLoss", false).format("console").queryName("kafka2console-contineous").trigger(Trigger.Continuous(120.seconds)).option("checkpointLocation", "/Users/keeratjohar2305/Downloads/SPARK_STRUCK_STREAM_WAL/EXP1").start()


//s2.awaitTermination
//df.isStreaming
//df.printSchema
//df.count()



//  val mySchema = StructType(Array(
//  StructField("id", IntegerType),
//  StructField("name", StringType),
//  StructField("year", IntegerType),
//  StructField("rating", DoubleType),
//  StructField("duration", IntegerType)
// ))



// import spark.implicits._
// val df = spark
//   .readStream
//   .format("kafka")
//   .option("kafka.bootstrap.servers", "localhost:9092")
//   .option("subscribe", "demo_test")
//   .load()



// val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
//   .select(from_json($"value", mySchema).as("data"), $"timestamp")
//   .select("data.*", "timestamp")



// df1.writeStream
//     .format("console")
//     .option("truncate","false")
//     .start()
//     .awaitTermination()
}
}