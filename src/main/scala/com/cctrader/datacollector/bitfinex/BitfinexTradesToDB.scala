package com.cctrader.datacollector.bitfinex

import java.io._
import java.net._

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.slick.jdbc.{StaticQuery => Q}
import scala.slick.lifted.TableQuery


/**
 * Get the newest trades from Bitstamp.
 * Used to breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.
 */
class BitfinexTradesToDB(dbWriter: DBWriter) {

  var lastTimestamp = dbWriter.getEndTime
  println("BitfinexTradesToDB: startTimestamp:" + lastTimestamp)

  val tickTable = TableQuery[TickTable]
  while (true) {
    try {
      val bitcoinchartsURL = new URL("https://api.bitfinex.com/v1/trades/btcusd?timestamp=" + (lastTimestamp - 1))
      val connection = bitcoinchartsURL.openConnection()
      val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
      var line: String = null
      val stringData = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
      var lastID = 0L
      bufferedReader.close()
      val children = parse(stringData).children
      children.reverse.foreach(x => {
        val timestamp = x.children(0).values.toString.toInt // ikke: 12345
        if (timestamp > lastTimestamp) {
          val id = None
          val sourceId = Some(x.children(1).values.toString.toLong)
          val price = x.children.apply(2).values.toString.toDouble
          val amount = x.children.apply(3).values.toString.toDouble
          dbWriter.newTick(TickDataPoint(id, sourceId, timestamp, price, amount))
          lastTimestamp = timestamp
        }
      })
      bufferedReader.close()
    }
    catch {
      case e: IOException => { e.printStackTrace(); e.toString() }
      case _ => println("Another exception")
    }
    dbWriter.isLive = true
    println("BitfinexTradesToDB: lastTimestamp:" + lastTimestamp + ". Waiting for 15 sec.")

    Thread.sleep(15000)
  }

}
