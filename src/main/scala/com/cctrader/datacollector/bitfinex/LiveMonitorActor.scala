package com.cctrader.datacollector.bitfinex

import akka.actor._

import scala.concurrent.duration._

/**
 *
 */
class LiveMonitorActor extends Actor {

  println("LiveMonitorActor: start")
  implicit val system = ActorSystem("actor-system-bitfinex")

  import system.dispatcher

  var liveActor = context.actorOf(Props[BitfinexTradesToDBActor])
  var schedule = context.system.scheduler.schedule(2 seconds, 15 seconds, liveActor, "GET TICKS")

  context.setReceiveTimeout(20 seconds)

  override def receive: Receive = {
    case "ALIVE" => {}
    case ReceiveTimeout => {
      println("LiveMonitorActor: Receive Timeout: restarts BitfinexTradesToDBActor")
      liveActor ! PoisonPill
      schedule.cancel()
      context.system.scheduler.scheduleOnce(15 seconds, self, "SET NEW LIVEACTOR")
    }
    case "SET NEW LIVEACTOR" => {
      liveActor = context.actorOf(Props[BitfinexTradesToDBActor])
      schedule = context.system.scheduler.schedule(2 seconds, 15 seconds, liveActor, "GET TICKS")
    }
  }

  override def postStop: Unit = {
    println("LiveMonitorActor: postStop")
    schedule.cancel()
    liveActor ! PoisonPill
  }

}
