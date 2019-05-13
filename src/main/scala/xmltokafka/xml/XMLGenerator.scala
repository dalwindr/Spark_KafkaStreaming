package xmltokafka.xml

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

/**
  * Created by root on 7/28/17.
  */

class XMLGenerator(xmlTemplate:String, config:String) {
  private val generalPattern = """.*>##(.*)##<.*""".r

  private val timestampPattern =""".*##TIMESTAMP\((.*)\)##.*""".r
  private val intPattern =""".*##RANDOM_INT\((\d+.*\d+)\)##.*""".r
  private val stringPattern =""".*##RANDOM_STRING\((.*)\)##.*""".r
  private val doublePattern =""".*##RANDOM_DOUBLE\((\d+.*\d+)\)##.*""".r
  private val booleanPattern =""".*##RANDOM_BOOLEAN\((.*)\)##.*""".r

  private val source = scala.io.Source.fromFile(xmlTemplate)

  private var template = source.getLines() mkString "\n"

  private val conf = new Conf(config)

  private val logger = LoggerFactory.getLogger(getClass)

  def start(): Unit ={

    val properties = new Properties()
    // comma separated list of Kafka brokers
    properties.setProperty("bootstrap.servers", s"${conf.broker}:${conf.port}")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key-class-type", "java.lang.String")
    properties.put("value-class-type", "java.lang.String")


    val producer = new KafkaProducer[String, String](properties)
    
    val ret = generalPattern.findAllIn(template).map { x =>

      val repLine = x match {
        case timestampPattern(x) => ("TIMESTAMP", conf.date_format)
        case intPattern(x) => ("INT", x )
        case stringPattern(x) => ("STRING", x)
        case doublePattern(x) =>("DOUBLE",x )
        case booleanPattern(x) => ("BOOLEAN", x )
      }
      //return (line, operation, values in the op)
      ( x, repLine._1, repLine._2)
    }.toList

    var i=0
    while ( i != conf.frequency){
      var message = template
      ret.foreach { x =>

        val randomValue = x._2 match {
          case "TIMESTAMP" =>  getTimestamp(conf.date_format)
          case "INT" => getInt(x._3).toString
          case "STRING" => getString(x._3).toString
          case "DOUBLE" => getDouble(x._3).toString
          case "BOOLEAN" => getBoolean.toString
          case _ => ""
        }
        message = message.replace(x._1, "##.*##".r.replaceAllIn(x._1, randomValue))
      }

      val record = new ProducerRecord(conf.topic, "key", message)
      producer.send(record)
      logger.info(message)
      i = i + 1
      Thread.sleep(conf.delay.toInt)
    }
  }

  def getInt(strInt:String):Integer ={
    val tmp = strInt.split(",")
    XMLRandomFunct.randomInteger(tmp(0).trim.toInt,tmp(1).trim.toInt)
  }

  def getString(strValues:String): String = {
    val tmp = strValues.split(",")
    XMLRandomFunct.randomString(tmp)
  }

  def getTimestamp(format:String): String ={
    new SimpleDateFormat(format).format(new Date())
  }

  def getDouble(strDoubles:String): Double={
    val tmp = strDoubles.split(",")
    XMLRandomFunct.randomDouble(tmp(0).trim.toDouble,tmp(1).trim.toDouble)
  }
  def getBoolean: Boolean ={
   XMLRandomFunct.randomBoolean()
  }

}