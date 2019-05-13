

import org.apache.spark.sql.SparkSession

object sparl1_XML_parser{

def main(args: Array[String]) {

 //import scala.xml.XML
// Download package for xml/avro
//spark-shell --master local[*] --packages com.databricks:spark-xml_2.10:0.4.1
// spark-shell --master local[*] --packages com.databricks:spark-avro_2.11:3.2.0 
//https://mvnrepository.com/artifact/com.databricks/spark-avro_2.11/3.2.0
//import com.databricks.spark.xml
//import com.databricks.spark.avro
 
//spark-shell --driver-class-path=/usr/local/Cellar/apache-spark/2.4.0/libexec/jars/*:/usr/local/Cellar/external-jars/*:/usr/local/Cellar/hbase-1.4.9/lib/*

    val blockSize = if (args.length > 2) args(2) else "4096"
    val spark = SparkSession
      .builder()
      .appName("XML testing")
      //.config("spark.broadcast.blockSize", blockSize)
      .getOrCreate()

var xml_name="/Users/keeratjohar2305/Downloads/books.xml"
var xmldf= spark.read.format("xml").option("rootTag","catalog").option("rowTag","book").option("inferSchema","true").load(xml_name)

xmldf.printSchema
xmldf.count()

//xmldf.write.format("parquet").save("book.parquet")
//xmldf.write.format("avro").save("book.avro")

}
}