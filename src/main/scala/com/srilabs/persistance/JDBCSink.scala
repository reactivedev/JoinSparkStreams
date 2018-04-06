package com.srilabs.persistance

import java.sql.Connection


import org.apache.spark.sql.ForeachWriter

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.Try

class  JDBCSink[T: TypeTag] extends ForeachWriter[T] {

  private var records  = mutable.ListBuffer.empty[T]

  // TODO: Persist batchId upon opening the connection???
  def open(partitionId: Long, version: Long): Boolean =  JDBCDriver.getConnection match {
    case Some(conn) =>
      Try(conn.close())
      true
    case None => false
  }

  def process(value: T): Unit = {
    records += value
  }

  def close(errorOrNull: Throwable): Unit = {
    if(records.nonEmpty) {
      val connection: Connection = JDBCDriver.getConnection.getOrElse(throw new Exception("Unable to connect"))
      try {
        //statement = connection.createStatement
        val statement = PreparedStatements.get[T](connection, records).get
        statement.executeBatch()
      } catch {
        case t: Throwable =>
          // error(s"Error executing statement type: ${typeOf[T]}", t)
      } finally {
        Try(connection.close())
        records = mutable.ListBuffer.empty[T]
      }
    }
  }
}
