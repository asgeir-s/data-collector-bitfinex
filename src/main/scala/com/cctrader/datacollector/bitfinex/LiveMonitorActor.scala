package com.cctrader.datacollector.bitfinex

import akka.actor.{ActorSystem, ReceiveTimeout, Props, Actor}
import akka.actor.Actor.Receive
import scala.concurrent.duration._

/**
 *
 */
class LiveMonitorActor(dbWriter: DBWriter) extends Actor {

  val liveActor = context.actorOf(BitfinexTradesToDBActor.props(dbWriter), "liveActor")

  implicit val system = ActorSystem("actor-system-bitfinex")
  import system.dispatcher

  context.system.scheduler.schedule(2 seconds, 15 seconds, liveActor, "GET TICKS")
  context.setReceiveTimeout(20 seconds)

  override def receive: Receive = {
    case "ALIVE" => {}
    case ReceiveTimeout => {
      println("Timeout received: restarts BitfinexTradesToDBActor")
      context.system.stop(liveActor)
      context.actorOf(BitfinexTradesToDBActor.props(dbWriter), "liveActor")
    }
  }
}

object LiveMonitorActor {
  def props(dbWriter: DBWriter): Props =
    Props(new LiveMonitorActor(dbWriter))
}
