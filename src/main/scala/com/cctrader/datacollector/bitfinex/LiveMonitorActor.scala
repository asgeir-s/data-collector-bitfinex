package com.cctrader.datacollector.bitfinex

import akka.actor._
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.slick.jdbc.JdbcBackend
import scala.slick.jdbc.JdbcBackend._

/**
 *
 */
class LiveMonitorActor extends Actor {

  println("LiveMonitorActor: start")
  implicit val system = ActorSystem("actor-system-bitfinex")

  import system.dispatcher

  val config = ConfigFactory.load()
  val databaseFactory: JdbcBackend.DatabaseDef = Database.forURL(
    url = "jdbc:postgresql://" + config.getString("postgres.host") + ":" + config.getString("postgres.port") + "/" + config
      .getString("postgres.dbname"),
    driver = config.getString("postgres.driver"),
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"))

  println("LiveMonitorActor: Create new DBWriter")
  val dbWriter = new DBWriter(databaseFactory, false)
  println("LiveMonitorActor: DBWriter initialized")

  var liveActor = context.actorOf(BitfinexTradesToDBActor.props(dbWriter))
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
      liveActor = context.actorOf(BitfinexTradesToDBActor.props(dbWriter))
      schedule = context.system.scheduler.schedule(2 seconds, 15 seconds, liveActor, "GET TICKS")
    }
  }

  override def postStop: Unit = {
    println("LiveMonitorActor: postStop")
    schedule.cancel()
    liveActor ! PoisonPill
  }

}
