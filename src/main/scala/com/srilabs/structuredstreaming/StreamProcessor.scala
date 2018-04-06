package com.srilabs.structuredstreaming

import com.srilabs.config.Settings
import com.srilabs.models.Activity
import org.apache.spark.sql.Encoders
import com.datastax.spark.connector.streaming._
import com.srilabs.persistance.JDBCSink

object StreamProcessor extends App {

  val spark = Settings.getSpark
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Settings.getKafkaBootstrapServers)
    .option("subscribe", Settings.getTopic)
    .load()

  implicit val encoder = Encoders.product[Activity]
  val objSchema = Encoders.product[Activity].schema
  objSchema.printTreeString()

  val table = df.select(from_json($"value".cast("string"), objSchema).as[Activity])
  table.printSchema()

  val writer = new JDBCSink[Activity]


  val query = table
    .writeStream
    .queryName("queryParams")
    .foreach(writer)
    .start()
    .awaitTermination()


// Using Direct Appender, support parquet, mongo. Seems JDBC & Cassandra are not YET supported
//  val query = table
//    .writeStream
//    .format("org.apache.spark.sql.cassandra")
//    .outputMode("append")
//    .option("table", "myTable")
//    .option("keyspace", "myKeySpace")
//    .start()
//    .awaitTermination()

}
