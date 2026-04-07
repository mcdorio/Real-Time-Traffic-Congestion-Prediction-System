# Real-Time Traffic Congestion Prediction System

## Overview
This project is a real-time data processing system that collects traffic data from the TomTom Traffic API, processes it using Apache Kafka and Apache Spark, and analyzes traffic congestion patterns.

The goal is to provide real-time traffic insights for commuters, transportation planners, and analysts.

---

## Tech Stack
- Scala
- Apache Kafka
- Apache Spark (Structured Streaming, MLlib, GraphX)
- TomTom Traffic API
- sbt

---

## Features
- Retrieve real-time traffic data from TomTom API
- Stream data into Kafka
- Process streaming data using Spark Structured Streaming
- Predict traffic congestion levels
- Analyze road networks using GraphX

---
## Setup Instructions

### 1. Clone the repository

### 2. Add your API key
Create a file, use example file as template

### 3. Run the project
In the terminal, run 'sbt "runMain TomTomKafkaProducer" '

# Real-Time Traffic Congestion Prediction System

## Overview
This project is a real-time data processing system that collects traffic data from the TomTom Traffic API, processes it using Apache Kafka and Apache Spark, and analyzes traffic congestion patterns.

The goal is to provide real-time traffic insights for commuters, transportation planners, and analysts.

---

## Tech Stack
- Scala
- Apache Kafka
- Apache Spark (Structured Streaming, MLlib, GraphX)
- TomTom Traffic API
- sbt

---

## Features
- Retrieve real-time traffic data from TomTom API
- Stream data into Kafka
- Process streaming data using Spark Structured Streaming
- Predict traffic congestion levels
- Analyze road networks using GraphX

---
## Setup Instructions

### 1. Clone the repository

### 2. Add your API key
Create a file, use example file as template


### 3. Run the project
In the terminal, run sbt "runMain TomTomKafkaProducer"

### 4. Verify Kafka messages
In a second terminal, run the following command
kafka-console-consumer.sh --topic traffic_events --from-beginning --bootstrap-server localhost:9092

- Currently sends one message at a time for testing and debugging; will be extended to continuous streaming in later iterations

### 5. Run Spark Streaming 
In the terminal, run sbt "runMain SparkIngest"
