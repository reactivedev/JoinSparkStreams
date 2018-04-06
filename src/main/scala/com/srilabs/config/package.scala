package com.srilabs

import java.util.Properties
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds}
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * A package object that contains common references to Config
  */
package object config {

  type Config = com.typesafe.config.Config

  /**
    * Common extension methods to extract Spark, Kafka configuration values and other common extensions based on config
    * @param config Typesafe config object
    */
  implicit class ConfigExtensions(config: Config) {

    /**
      * validate one or more paths for if there exists a vale or not
      * @param paths config path
      */
    def validate(paths: String*): Unit = paths.foreach(path => {
      require(config.hasPath(path), s"Missing configuration for path $path")
    })

    /**
      * Extract config List as CSV string
      * @param path config path
      * @return CSV string
      */
    def getSeqAsCSV(path: String): String = config.as[Seq[String]](path).mkString(",")

    /**
      * a safer way to extract values for optional configuration values
      * USE WITH CAUTION, We prefer Fail Fast approach, use only if you have default values in mind
      * @param path config path
      * @return Option[T] A scala optional value --- Some(T) or None
      */
    def tryGetString(path: String): Option[String] = Try(config.getString(path)).toOption

    /**
      * Get configuration properties as Map[String, String] for example Kafka Properties
      * @param path path
      * @return Map[String, String]
      */
    def getMap(path: String): Map[String, String] = config.as[Map[String, String]](path)

    /**
      * Load nested properties as Map, for example spark.streaming.duration may be nested inside config but we really want key & value
      * @param path path to root of nested properties
      * @param keyPrefix initial value (if any)
      * @return Map[String, String]
      */
    def nestedConfigAsMap(path: String, keyPrefix: String = ""): Map[String, String] = {
      val prefix = if(!keyPrefix.isEmpty) keyPrefix + "." else ""
      config
        .getObject(path).toConfig.entrySet().asScala
        .map(x => (prefix + x.getKey, x.getValue.unwrapped().toString)).toMap
    }

    /**
      * Get config as Map if exists
      * @param path path
      * @return Map[String, String]
      */
    def tryGetMap(path: String): Option[Map[String, String]] = Try(getMap(path)).toOption

    /**
      * A utility method to extract Spark streaming duration from config
      * @param path config path
      * @return spark streaming duration
      */
    def getStreamDuration(path: String): Duration = Seconds(config.getInt(path))

    /**
      * A utility method to instantiate spark session by extracting SparkConfig from config
      * @param path spark configuration path
      * @return Spark Session
      */
    def getSpark(path: String): SparkSession = {
      val spark = SparkSession.builder.config(getConf(path)).getOrCreate()
      //debug(Mapper.toJson(spark.conf.getAll))
      spark
    }

    /**
      * A utility method to extract SparkConfig from config
      * @param sparkRootPath config path
      * @return SparkConfig
      */
    def getConf(sparkRootPath: String): SparkConf = {
      val master = tryGetString(sparkRootPath + ".master")
      val appName = config.getString(sparkRootPath + ".name")
      val sparkConf = new SparkConf()

      sparkConf.setAppName(appName)
      if(master.isDefined) sparkConf.setMaster(master.get)

      if(config.hasPath(sparkRootPath + ".streaming")) {
        val streamingProps = config.nestedConfigAsMap(sparkRootPath + ".streaming", "spark.streaming")
        streamingProps.foreach(x =>
          sparkConf.set(x._1, x._2)
        )
      }

      if(config.hasPath(sparkRootPath + ".sql")) {
        val streamingProps = config.nestedConfigAsMap(sparkRootPath + ".sql", "spark.sql")
        streamingProps.foreach(x =>
          sparkConf.set(x._1, x._2)
        )
      }

      sparkConf
    }

    def kafkaBootstrapServers(kafkaRootPath: String): String = getSeqAsCSV(kafkaRootPath + ".bootstrap-servers")

    def kafkaProducerConfig(kafkaRootPath: String): Map[String, String] = {
      config.as[Map[String, String]](kafkaRootPath + ".properties") ++
        Map("bootstrap.servers" -> kafkaBootstrapServers(kafkaRootPath))
    }

    /**
      * A utility method to extract JDBC connection properties from config
      * @param path config path
      * @return Java Properties as needed for JDBC connection
      */
    def sqlProps(path: String): Properties = config.as[Map[String, String]](path)

    /**
      * Translate Scala Map[String, String] to Java properties
      * @param properties scala Map[String, String]
      * @return java Properties -- [String, String]
      */
    implicit def propsFromMap(properties: Map[String, String]): Properties =
      (new Properties /: properties) {
        case (a, (k, v)) =>
          a.put(k,v)
          a
      }

  }
}
