//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named “SimpleProducer”



import java.util.concurrent.Future

import org.apache.kafka.clients.producer.RecordMetadata
  import org.apache.kafka.clients.producer.ProducerRecord
object KafkaProducer extends App {

  val topic = util.Try(args(0)).getOrElse("demo_test")
  println(s"Connecting to $topic")

  import org.apache.kafka.clients.producer.KafkaProducer

  val props = new java.util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

 


   val producer = new KafkaProducer[Integer, String](props)
   import org.apache.kafka.clients.producer.ProducerRecord
  
   while (true)
   {
    Thread.sleep(1000)
  for (j <- 1 until  500000 ) {

  val polish = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm:ss")
  val now = java.time.LocalDateTime.now().format(polish)
 
  val record = new ProducerRecord[Integer, String](topic, 1, s"hello at $now")

    val metaF: Future[RecordMetadata] = producer.send(record)
   
  val meta = metaF.get() // blocking!
  val msgLog =
    s"""
       |offset    = ${meta.offset()}
       |partition = ${meta.partition()}
       |topic     = ${meta.topic()}
     """.stripMargin
  println(msgLog)
  }
}
  producer.close()

  /*

     for(int i = 0; i < 10; i++)
         producer.send(new ProducerRecord<String, String>(topicName, 
            Integer.toString(i), Integer.toString(i)));
               System.out.println(“Message sent successfully”);
               producer.close();

  */

}
