//Spark reads the Kafka messages and streams a dataframe
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkIngest {
  def main(args: Array[String]): Unit = {

    // Load config file
    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    //Read Kafka settings from config
    val bootstrapServers = config.getString("kafka.bootstrapServers")
    val topic = config.getString("kafka.topic")

    //Create Spark session
    val spark = SparkSession.builder()
      .appName("KafkaToSparkIngest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //Read streaming data from Kafka
    val kafkaDB = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    //Kafka key/value come in as bytes, so convert them to strings
    val jsonDf = kafkaDB.selectExpr(
      "CAST(key AS STRING) as message_key",
      "CAST(value AS STRING) as json_str",
      "topic",
      "partition",
      "offset",
      "timestamp"
    )

    //Define the schema of the JSON being read from Kafka
    val trafficSchema = StructType(Seq(
      StructField("point", StringType, true),
      StructField("currentSpeed", DoubleType, true),
      StructField("freeFlowSpeed", DoubleType, true),
      StructField("currentTravelTime", IntegerType, true),
      StructField("freeFlowTravelTime", IntegerType, true),
      StructField("confidence", DoubleType, true),
      StructField("roadClosure", BooleanType, true),
      StructField("frc", StringType, true)
    ))

    //Parse the JSON string into structured columns
    val trafficDf = jsonDf
      .select(
        col("message_key"),
        from_json(col("json_str"), trafficSchema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )
      .select(
        col("message_key"),
        col("data.*"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )

    //Write the streaming DataFrame to the console
    val query = trafficDf.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}