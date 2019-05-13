object DstreamXMLParsing{
def main(args: Array[String]) {

   import org.apache.spark._
   import org.apache.spark.streaming._
   import org.apache.spark.streaming.{Seconds, StreamingContext}
   import org.apache.spark.sql.SparkSession
   val spark = SparkSession.builder.appName("Spark-Kafka-Integration").master("local").getOrCreate()
   import spark.implicits._ // use along with sparkSession context		

   val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
   val ssc = new StreamingContext(conf, Seconds(3))   // new context
   var lines = ssc.socketTextStream("localhost",9999)
   //lines.foreachRDD{rdd=> val spark = SparkSession.builder.appName("Spark-Kafka-Integration").master("local").getOrCreate();import spark.implicits._; var xmlrdd = new com.databricks.spark.xml.XmlReader().xmlRdd(spark.sqlContext,rdd); xmlrdd.createOrReplaceTempView("test");spark.sql("select food.calories,food.description,food.name,food.price from test").show();spark.sql("describe table test").show(false)}
   lines.foreachRDD{rdd=> val spark = SparkSession.builder.appName("Spark-Kafka-Integration").master("local").getOrCreate();import spark.implicits._; var xmlrdd = new com.databricks.spark.xml.XmlReader().xmlRdd(spark.sqlContext,rdd); xmlrdd.select($"food.calories").show(false)}
   ssc.start()
   ssc.stop()
}
}