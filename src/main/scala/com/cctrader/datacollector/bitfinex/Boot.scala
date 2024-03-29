package com.cctrader.datacollector.bitfinex

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.slick.jdbc.JdbcBackend
import scala.slick.jdbc.JdbcBackend._

/**
 * Used to start the actor system
 */

object Boot extends App {

  val config = ConfigFactory.load()

  val databaseFactory: JdbcBackend.DatabaseDef = Database.forURL(
    url = "jdbc:postgresql://" + config.getString("postgres.host") + ":" + config.getString("postgres.port") + "/" + config
      .getString("postgres.dbname"),
    driver = config.getString("postgres.driver"),
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"))

  val dbSession = databaseFactory.createSession()


  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - Start --------------------------")
  new BitcoinChartsHistoryToDB(false, false, false, "", dbSession)
  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - end ----------------------------")

  println("-------------------------- STEP2 - BfxDataHistoryToDB - Start --------------------------------")
  new BFXdataTradesToDB(dbSession)
  dbSession.close()
  println("-------------------------- STEP2 - BfxDataHistoryToDB - end ----------------------------------")

  println("-------------------------- STEP3 - BitfinexLive - Start --------------------------------------")
  implicit val system = ActorSystem("actor-system-bitfinex")
  val liveMonitorActor = system.actorOf(Props[LiveMonitorActor])

}