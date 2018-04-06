package com.srilabs.persistance

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import com.srilabs.config.Settings

object JDBCDriver {

  private[srilabs] def getConnection: Option[Connection] = try {
    val sqlProps = Settings.getSqlProps
    Class.forName(sqlProps("driver"))
    Some(DriverManager.getConnection( sqlProps("url"), sqlProps("username"), sqlProps("password")))
  } catch {
    case e: SQLException =>
      //Log.fatal("JDBC Connection Exception", e)
      None
  }


  private[srilabs] def ExecuteStatement[T](query: String, f: (ResultSet) => T): T = getConnection match {
    case Some(conn) =>
      try {
        f(conn.createStatement().executeQuery(query))
      }
      finally {
        conn.close()
      }
  }
}