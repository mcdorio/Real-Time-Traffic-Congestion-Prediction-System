
import com.typesafe.config.ConfigFactory
import io.circe.parser._
import requests._

object TomTomApiTest {
  def main(args: Array[String]): Unit = {
    
    val config = ConfigFactory.parseFile(
    new java.io.File("src/main/resources/application.conf")
    )

    val apiKey = config.getString("tomtom.apiKey")
    val point = config.getString("tomtom.point")

    val url =
      s"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" +
        s"?key=$apiKey&point=$point&unit=mph"


    val response = requests.get(url, readTimeout = 30000)

    if (response.statusCode != 200) {
      println(s"Error: ${response.statusCode}")
      println(response.text())
      sys.exit(1)
    }

    val jsonString = response.text()
    println(jsonString)

    val json = parse(jsonString).getOrElse {
      throw new Exception("Failed to parse JSON")
    }

    val cursor = json.hcursor.downField("flowSegmentData")

    val currentSpeed = cursor.get[Double]("currentSpeed").getOrElse(0.0)
    val freeFlowSpeed = cursor.get[Double]("freeFlowSpeed").getOrElse(0.0)
    val currentTravelTime = cursor.get[Int]("currentTravelTime").getOrElse(0)
    val freeFlowTravelTime = cursor.get[Int]("freeFlowTravelTime").getOrElse(0)

    println("\nExtracted Data:")
    println(s"Current Speed: $currentSpeed")
    println(s"Free Flow Speed: $freeFlowSpeed")
    println(s"Current Travel Time: $currentTravelTime")
    println(s"Free Flow Travel Time: $freeFlowTravelTime")
  }
}