package com.cctrader.datacollector.bitfinex

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import scala.slick.jdbc.JdbcBackend._

/**
 * Used to start the actor system
 */

object Boot extends App {

  val config = ConfigFactory.load()

  val databaseFactory = Database.forURL(
    url = "jdbc:postgresql://" + config.getString("postgres.host") + ":" + config.getString("postgres.port") + "/" + config
      .getString("postgres.dbname"),
    driver = config.getString("postgres.driver"),
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"))

  val dbSession = databaseFactory.createSession()


  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - Start --------------------------")
  new BitcoinChartsHistoryToDB(false, false, false, "/Users/asgeir/Dropbox/Master/System/BitfinexCollector/download/bitfinexUSD1411991236758.csv", dbSession)
  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - end ----------------------------")

  println("-------------------------- STEP2 - BfxDataHistoryToDB - Start --------------------------------")
  new BFXdataTradesToDB(dbSession)
  println("-------------------------- STEP2 - BfxDataHistoryToDB - end ----------------------------------")

  println("-------------------------- STEP3 - DBWriter - Start ------------------------------------------")
  val dbWriter = new DBWriter(dbSession, false)
  println("-------------------------- STEP3 - DBWriter - Initialization done ----------------------------")

  println("-------------------------- STEP4 - BitfinexLive - Start --------------------------------------")
  implicit val system = ActorSystem("actor-system")
  val liveActor = system.actorOf(BitfinexTradesToDBActor.props(dbWriter))
  println("-------------------------- STEP4 - BitfinexLive - end ----------------------------------------")

}