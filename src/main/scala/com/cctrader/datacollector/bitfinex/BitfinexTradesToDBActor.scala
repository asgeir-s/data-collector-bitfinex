package com.cctrader.datacollector.bitfinex

import java.io._
import java.net._
import java.util.Date

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.slick.jdbc.JdbcBackend._
import scala.slick.jdbc.{JdbcBackend, StaticQuery => Q}


/**
 * Get the newest trades from Bitstamp.
 * Used to breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.
 */
class BitfinexTradesToDBActor extends Actor {

  println("BitfinexTradesToDBActor: start")
  context.parent ! "ALIVE"
  val config = ConfigFactory.load()

  val databaseFactory: JdbcBackend.DatabaseDef = Database.forURL(
    url = "jdbc:postgresql://" + config.getString("postgres.host") + ":" + config.getString("postgres.port") + "/" + config
      .getString("postgres.dbname"),
    driver = config.getString("postgres.driver"),
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"))

  println("BitfinexTradesToDBActor: Create new DBWriter")
  val dbWriter = new DBWriter(databaseFactory, false)
  var lastTimestamp = dbWriter.getEndTime
  println("BitfinexTradesToDB: startTimestamp:" + lastTimestamp)

  var first = true
  var bitcoinchartsURL: URL = _
  var bufferedReader: BufferedReader = _
  var stringData: String = _
  var children: List[JsonAST.JValue] = _

  context.parent ! "ALIVE"
  println("BitfinexTradesToDBActor: Initialization finished. Waiting for \"GET TICKS\"")

  override def receive: Receive = {
    case "GET TICKS" => {
      bitcoinchartsURL = new URL("https://api.bitfinex.com/v1/trades/btcusd?timestamp=" + (lastTimestamp - 1))
      bufferedReader = new BufferedReader(new InputStreamReader(bitcoinchartsURL.openConnection().getInputStream))
      stringData = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
      bufferedReader.close()
      children = parse(stringData).children
      children.reverse.foreach(x => {
        if (x.children(0).values.toString.toInt > lastTimestamp) {
          dbWriter.newTick(TickDataPoint(None, Some(x.children(1).values.toString.toLong), x.children(0).values.toString.toInt, x.children.apply(2).values.toString.toDouble, x.children.apply(3).values.toString.toDouble))
          lastTimestamp = x.children(0).values.toString.toInt
          if (first) {
            dbWriter.isLive = true
            first = false
          }
        }
      })
      println("BitfinexTradesToDB: last tick:" + new Date(lastTimestamp.toLong * 1000L) + ". Waiting for 15 sec.")
      context.parent ! "ALIVE"
    }
  }

  override def postStop: Unit = {
    println("BitfinexTradesToDBActor: Something went wrong. BitfinexTradesToDB will restart. This does not effect execution :)")
    dbWriter.closeDBConnection
  }
}