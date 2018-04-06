package com.srilabs

import com.srilabs.util.Mapper

package object models {

  case class Activity(
                       timeStamp: Long,
                       productId: Int,
                       userId: Int,
                       referrer: String,
                       retailPrice: Int,
                       productDiscountPct: Int,
                       cartDiscountPct: Int,
                       actionCode: Int,
                       marginPct: Int
                     ) {
    def toJson: String = Mapper.toJson(this)
  }

  object Activity {

    def create(
               timeStamp: Long,
               productId: Int,
               userId: Int,
               referrer: String,
               retailPrice: Int,
               productDiscountPct: Int,
               cartDiscountPct: Int,
               actionCode: Int,
               marginPct: Int
             ): Activity = new Activity(timeStamp, productId, userId, referrer, retailPrice, productDiscountPct, cartDiscountPct, actionCode, marginPct)

    def get(str: String): Activity = Mapper.fromJson[Activity](str)

  }

  case class ProductActivityByReferrer(
                                      productId: Int,
                                      referrer: String,
                                      timeStamp: Long,
                                      viewCount: Long,
                                      cartCount: Long,
                                      purchaseCount: Long
                                      )
}
