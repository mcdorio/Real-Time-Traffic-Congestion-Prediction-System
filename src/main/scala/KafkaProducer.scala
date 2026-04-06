
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.syntax._
import io.circe.parser._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import requests._

import java.util.Properties

object TomTomKafkaProducer {
  def main(args: Array[String]): Unit = {

    //load config file
    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    //read values from config file 
    val apiKey = config.getString("tomtom.apiKey")
    val point = config.getString("tomtom.point")
    val bootstrapServers = config.getString("kafka.bootstrapServers")
    val topic = config.getString("kafka.topic")

    //Kafka configuration properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    //create Kafka producer object
    val producer = new KafkaProducer[String, String](props)

    try {
        //TomTom API URL using API key and location
      val url =
        s"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" +
          s"?key=$apiKey&point=$point&unit=mph"

      println("Calling TomTom API...")

    //send HTTP GET request to TomTom api
      val response = requests.get(
        url,
        readTimeout = 30000,
        connectTimeout = 10000
      )

      if (response.statusCode != 200) {
        println(s"TomTom API Error: ${response.statusCode}")
        println(response.text())
        sys.exit(1)
      }

    //gets response as raw JSON text
      val rawJson = response.text()

    //parse JSON string into object
      val parsed = parse(rawJson).getOrElse {
        throw new Exception("Failed to parse TomTom JSON")
      }

      val cursor = parsed.hcursor.downField("flowSegmentData")

    //extract values from JSON
      val currentSpeed = cursor.get[Double]("currentSpeed").getOrElse(0.0)
      val freeFlowSpeed = cursor.get[Double]("freeFlowSpeed").getOrElse(0.0)
      val currentTravelTime = cursor.get[Int]("currentTravelTime").getOrElse(0)
      val freeFlowTravelTime = cursor.get[Int]("freeFlowTravelTime").getOrElse(0)
      val confidence = cursor.get[Double]("confidence").getOrElse(0.0)
      val roadClosure = cursor.get[Boolean]("roadClosure").getOrElse(false)
      val frc = cursor.get[String]("frc").getOrElse("UNKNOWN")

    //JSON object to send to kafka
      val kafkaMessage: Json = Json.obj(
        "point" -> point.asJson,
        "currentSpeed" -> currentSpeed.asJson,
        "freeFlowSpeed" -> freeFlowSpeed.asJson,
        "currentTravelTime" -> currentTravelTime.asJson,
        "freeFlowTravelTime" -> freeFlowTravelTime.asJson,
        "confidence" -> confidence.asJson,
        "roadClosure" -> roadClosure.asJson,
        "frc" -> frc.asJson
      )

    //JSON object to string 
      val messageString = kafkaMessage.noSpaces

      println("Sending message to Kafka...")
      println(messageString)

    //create Kafka message
      val record = new ProducerRecord[String, String](topic, point, messageString)
    //send message to kafka
      val metadata = producer.send(record).get()

      println(s"Sent to topic: ${metadata.topic()}")
      println(s"Partition: ${metadata.partition()}")
      println(s"Offset: ${metadata.offset()}")

    } finally {
      producer.flush()
      producer.close()
    }
  }
}