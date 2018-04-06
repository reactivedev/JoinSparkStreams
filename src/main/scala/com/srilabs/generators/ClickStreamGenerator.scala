package com.srilabs.generators

import java.io.FileWriter
import com.srilabs.models._
import com.srilabs.util._
import scala.util.Random

object ClickStreamGenerator extends App {

  import com.srilabs.config.Settings._
  val rnd = new Random()

  getTargetDirectories.foreach { d => {

    val products = getProducts()
    val fw = new FileWriter(s"$d/${System.currentTimeMillis() + ".data"}", true)

    for(cnt <- 1 to batchSize){

        val productId = productRangeStart + rnd.nextInt((productRangeEnd - productRangeStart) + 1)
        val customerId = userRangeStart + rnd.nextInt((userRangeEnd - userRangeStart) + 1)
        val margin = getProductMargin(productId)
        val activity = if(cnt % promoAvailabilityFactor > 0){
          val pd = getProductDiscount(productId)
          val cd = getCartDiscount(productId)
          Activity.create(System.currentTimeMillis(), productId, customerId, getRandomReferrer(), products(productId), pd, cd, getAction(pd, cd), margin)
        } else {
          Activity.create(System.currentTimeMillis(), productId, customerId, "site", products(productId), 0, 0, getAction(), margin)
        }
        val jsonString = Mapper.toJson(activity) + System.lineSeparator()
        fw.append(jsonString)

    }

  fw.close()
  }}

}