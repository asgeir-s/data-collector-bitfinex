package com.cctrader.datacollector.bitfinex

import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.Date

import com.typesafe.config.ConfigFactory

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.StaticQuery.interpolation
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.{StaticQuery => Q}

/**
 * Downloads full history (zip with csv-file) from Bitcoin Charts and writes it to the database.
 *
 * UNIX: requires /usr/bin/gzip command.
 */
class BitcoinChartsHistoryToDB(download: Boolean, decompress: Boolean,
                               writeToDB: Boolean,
                               csvFilPath: String, implicit var session: Session) {
  val config = ConfigFactory.load()
  val bitcoinchartsURL = new URL("http://api.bitcoincharts.com/v1/csv/bitfinexUSD.csv.gz")
  val compressedHistoryFile = "download/bitfinexUSD" + new java.util.Date().getTime + ".csv.gz"
  val decompressedHistoryFile = {
    if (csvFilPath.length == 0)
      compressedHistoryFile.substring(0, compressedHistoryFile.length - 3)
    else
      csvFilPath
  }

  if (download) {
    println("Download start")
    val connection = bitcoinchartsURL.openConnection()
    Files.copy(connection.getInputStream, Paths.get(compressedHistoryFile))
    println("Download finished")
  }

  if (decompress) {
    println("Decompress start")
    Runtime.getRuntime.exec("/usr/bin/gzip -df " + compressedHistoryFile).waitFor //Unix
    println("Decompress Finished")
  }

  if (writeToDB) {
    val tickTable = TableQuery[TickTable]
    // remove table if it exists
    if (makeTableMap.contains("bitfinex_btc_usd_tick"))
      tickTable.ddl.drop
    tickTable.ddl.create

    println("Writing to database start")
    val filPath = Paths.get(decompressedHistoryFile).toAbsolutePath.toString
    sql"COPY bitfinex_btc_usd_tick(timestamp, price, amount) FROM '#$filPath' DELIMITER ',' CSV;".as[String].list
    println("Writing to database finish")


    var startTime: Date = {
      val list = tickTable.sortBy(_.id).list
      list(0).date
    }

    var endTime = {
      val list = tickTable.sortBy(_.id).list
      list.last.date
    }
    println("BitcoinChartsHistoryToDB: startTimestamp:" + startTime.getTime / 1000 + ", endTimestamp:" + endTime.getTime / 1000)

  }
  else {
    println("BitcoinChartsHistoryToDB: Did not get any new dataPoints. writeToDB==false")

  }

  def makeTableMap: Map[String, MTable] = {
    val tableList = MTable.getTables.list(session)
    val tableMap = tableList.map { t => (t.name.name, t)}.toMap
    tableMap
  }

}