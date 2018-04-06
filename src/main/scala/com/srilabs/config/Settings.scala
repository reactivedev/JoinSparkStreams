package com.srilabs.config

import java.lang.management.ManagementFactory
import java.util.Properties

import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object Settings {
  val config = ConfigFactory.load()

  private val settings = config.getConfig("sri.gen")

  lazy val appName = settings.getString("app-name")
  lazy val sparkMaster = settings.getString("spark-master")
  lazy val numCores = settings.getString("spark-number-of-cores")

  lazy val batchSize = settings.getInt("batch-size")
  lazy val numBatches = settings.getInt("number-of-batches")
  lazy val productRangeStart = settings.getInt("product-range-start")
  lazy val productRangeEnd = settings.getInt("product-range-end")
  lazy val userRangeStart = settings.getInt("user-range-start")
  lazy val userRangeEnd = settings.getInt("user-range-end")
  lazy val promoAvailabilityFactor = settings.getInt("promo-availability-factor")
  lazy val messageDelay = settings.getInt("message-depaly-ms")
  lazy val streamDelay = settings.getInt("stream-delay-ms")

  def getTargetDirectories= settings.getStringList("target-directories").asScala.toList
  def getSpark: SparkSession = config.getSpark("sri.spark")
  def getKafkaBootstrapServers = config.kafkaBootstrapServers("sri.kafka")
  def getTopic = config.getString("sri.kafka.topic")
  def getSqlProps: Map[String, String] = config.getMap("sri.jdbc")

}
