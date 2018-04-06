package com.srilabs.persistance

import java.sql.{Connection, PreparedStatement, Timestamp}

import com.srilabs.models.{Activity, ProductActivityByReferrer}

import scala.reflect.runtime.universe._

object PreparedStatements {

  def get[T: TypeTag](con: Connection, logLines: Seq[T]): Option[PreparedStatement] = typeOf[T] match {
    case  t if t =:= typeOf[Activity] =>
        val sql =
          """insert into dbo.activities
          (
          timeStamp, productId, userId, referrer, retailPrice, productDiscountPct, cartDiscountPct, actionCode, marginPct
          )
          VALUES (
         ?,?,?,?,?,?,?,?,?
          )""".stripMargin
            val statement = con.prepareStatement(sql)
      logLines.foreach { l =>
              val line = l.asInstanceOf[Activity]
              statement.setTimestamp(1, new Timestamp(line.timeStamp))
              statement.setInt(2, line.productId)
              statement.setInt(3, line.userId)
              statement.setString(4, line.referrer)
              statement.setInt(5, line.retailPrice)
              statement.setInt(6, line.productDiscountPct)
              statement.setInt(7, line.cartDiscountPct)
              statement.setInt(8, line.actionCode)
              statement.setInt(9, line.marginPct)
              statement.addBatch()
            }
      Some(statement)
    case  t if t =:= typeOf[ProductActivityByReferrer] =>
      val sql =
        """insert into web.visit_tmp
          (
          productId, referrer, timeStamp, viewCount, cartCount, purchaseCount
          )
          VALUES (
         ?,?,?,?,?,?
          )""".stripMargin
      val statement = con.prepareStatement(sql)
      logLines.foreach { l =>
        val line = l.asInstanceOf[ProductActivityByReferrer]
        statement.setInt(1, line.productId)
        statement.setString(2, line.referrer)
        statement.setLong(3, line.timeStamp)
        statement.setLong(4, line.viewCount)
        statement.setLong(5, line.cartCount)
        statement.setLong(6, line.purchaseCount)
        statement.addBatch()
      }
      Some(statement)
    case _ => None
  }
}

