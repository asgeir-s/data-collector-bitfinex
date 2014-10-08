package com.cctrader.datacollector.bitfinex

import java.io._
import java.net._
import java.util.Date

import akka.actor.{ActorSystem, _}
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import spray.client.pipelining._
import spray.httpx.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import spray.util._

import scala.concurrent.duration._
import scala.slick.jdbc.{StaticQuery => Q}
import scala.util.{Failure, Success}

// {"timestamp":1412786718,"tid":3437689,"price":"344.5","amount":"0.1074","exchange":"bitfinex","type":"sell"}
object MyJsonProtocol extends DefaultJsonProtocol {

  implicit object TickDataPointFormat extends RootJsonFormat[TickDataPoint] {
    override def read(json: JsValue): TickDataPoint = {
      json.asJsObject.getFields("timestamp", "price", "amount", "tid") match {
        case Seq(JsNumber(timestamp), JsString(price), JsString(amount), JsNumber(tid)) =>
          new TickDataPoint(None, Some(tid.toInt), timestamp.toInt, price.toDouble, amount.toDouble)
        case _ => throw new DeserializationException("TickDataPoint expected.")
      }
    }

    override def write(obj: TickDataPoint): JsValue = {
      JsObject(
        "id" -> JsNumber(obj.id.get)
      )
    }
  }

}

/**
 * Get the newest trades from Bitstamp.
 * Used to breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.
 */
class BitfinexTradesToDBActor(dbWriter: DBWriter) extends Actor with ActorLogging {

  implicit val system = ActorSystem("actor-system-bitfinex")
  import system.dispatcher

  println("BitfinexTradesToDBActor: start")
  var lastTimestamp = dbWriter.getEndTime
  println("BitfinexTradesToDB: last timestamp in database:" + lastTimestamp)

  var first = true

  import com.cctrader.datacollector.bitfinex.MyJsonProtocol._
  import spray.httpx.SprayJsonSupport._

  val pipeline = sendReceive ~> unmarshal[List[TickDataPoint]]

  context.parent ! "ALIVE"
  println("BitfinexTradesToDBActor: Initialization finished. Waiting for \"GET TICKS\"")

  override def receive: Receive = {
    case "GET TICKS" => {
      val responseFuture = pipeline {
        Get("https://api.bitfinex.com/v1/trades/btcusd?timestamp=" + (lastTimestamp+1))
      }
      responseFuture onComplete {
        case Success(dataPointList) =>
          dataPointList.sortBy(_.sourceId).foreach(x => {
            dbWriter.newTick(x)
          })

          if(first) {
            dbWriter.isLive = true
            first = false
          }

          if(dataPointList.size > 0) {
            lastTimestamp = dataPointList.sortBy(_.sourceId).last.timestamp
          }

        case Success(somethingUnexpected) =>
          log.warning("The BitfinexT API call was successful but returned something unexpected: '{}'.", somethingUnexpected)

        case Failure(error) =>
          log.error(error, "Some Failure from BitfinexT API")

      }
      println("BitfinexTradesToDB: last tick:" + new Date(lastTimestamp.toLong * 1000L) + ". Waiting for 15 sec.")
      context.parent ! "ALIVE"
    }
  }

  override def postStop: Unit = {
    println("BitfinexTradesToDBActor: postStop")
    IO(Http).ask(Http.CloseAll)(1.second).await
    system.shutdown()
  }

  override def preStart: Unit = {
    println("BitfinexTradesToDBActor: preStart")
    dbWriter.resetDBConnection
  }
}

object BitfinexTradesToDBActor {
  def props(dbWriter: DBWriter): Props =
    Props(new BitfinexTradesToDBActor(dbWriter))
}