package com.cctrader.datacollector.bitfinex

import java.io._
import java.net._

import akka.actor.{ActorSystem, Actor, Props}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.slick.jdbc.{StaticQuery => Q}
import scala.concurrent.duration._



/**
 * Get the newest trades from Bitstamp.
 * Used to breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.
 */
class BitfinexTradesToDBActor(dbWriter: DBWriter) extends Actor {

  var lastTimestamp = dbWriter.getEndTime
  println("BitfinexTradesToDB: startTimestamp:" + lastTimestamp)

  var first = true
  var bitcoinchartsURL: URL = _
  var bufferedReader: BufferedReader = _
  var stringData: String = _
  var children: List[JsonAST.JValue] = _

  implicit val system = ActorSystem("actor-system")
  import system.dispatcher
  context.system.scheduler.scheduleOnce(15 seconds, self, "GET TICKS")

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println("Something went wrong. Will restart actor. This does not effect execution :)")
    super.preRestart(reason, message)
  }

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
      println("BitfinexTradesToDB: lastTimestamp:" + lastTimestamp + ". Waiting for 15 sec.")
      context.system.scheduler.scheduleOnce(15 seconds, self, "GET TICKS")
    }
  }
}

object BitfinexTradesToDBActor {
  def props(dbWriter: DBWriter): Props =
    Props(new BitfinexTradesToDBActor(dbWriter))
}