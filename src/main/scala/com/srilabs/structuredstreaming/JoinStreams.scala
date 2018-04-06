package com.srilabs.structuredstreaming

import com.srilabs.config.Settings
import com.srilabs.logging.DirectLogger
import com.srilabs.models.Activity
import org.apache.spark.sql.Encoders

object JoinStreams extends App {

  val spark = Settings.getSpark
  import spark.implicits._

  val activityStream1 = getStream(Settings.getTargetDirectories(0))
  val activityStream2 = getStream(Settings.getTargetDirectories(1))

  activityStream1
    .union(activityStream2)
    .map(x => {
      val str = x.toJson
      DirectLogger.logger.info(str)
      str
    })
    .alias("value")
    .writeStream
    //.format("console")
    .format("kafka")
    .option("kafka.bootstrap.servers", Settings.getKafkaBootstrapServers)
    .option("topic", Settings.getTopic)
    .start()
    .awaitTermination()


  private def getStream(path: String) = {
    val stream = spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .load(path)

    val lines = stream.select($"value".cast("string")).as[String]
    implicit val activityEncoder = Encoders.product[Activity]

    lines.map { str =>
      Activity.get(str)
    }
  }


//  activityStream1
//    .union(activityStream2)
//    .writeStream
//    //.format("console")
//    .format("kafka")
//    .option("kafka.bootstrap.servers", Settings.getKafkaBootstrapServers)
//    .option("topic", Settings.getTopic)
//    .start()
//    .awaitTermination()


//  private def getStream(path: String) = {
//    spark.readStream
//      .format("text")
//      .option("maxFilesPerTrigger", 1)
//      .load(path)
//  }

}