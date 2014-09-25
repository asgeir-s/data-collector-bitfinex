package com.cctrader.datacollector.bitfinex

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.{StaticQuery => Q}

/**
 * Get "new" trades from Bitcoincharts.
 * The trades retrieved are delayed by approx. 15 minutes
 */
class BfxDataTradesToDB(sessionIn: Session) {

  implicit var session: Session = sessionIn
  val tickTable = TableQuery[TickTable]

  var endTime: Int = {
      val lengthString = tickTable.length.run
      val lastRow = tickTable.filter(x => x.id === lengthString.toLong).take(1)
      val value = lastRow.firstOption map (x => x.timestamp)
      value.get
  }

  val BfxDataURL = new URL("http://www.bfxdata.com/json/lastTradesBTCUSD.json")
  val connection = BfxDataURL.openConnection()
  val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
  var line: String = null
  val str = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
  bufferedReader.close()
  val stringArray = str.split(",\\[")
  stringArray.foreach(x => {
    val pointData = x.substring(x.lastIndexOf('[')+1, x.indexOf(']')).split(",")
    val tick = TickDataPoint(None, None, pointData(0).substring(0, pointData(0).length - 3).toInt, pointData(1).toDouble, 1)
    if (tick.timestamp > endTime) {
      tickTable += tick
    }
  })

  def makeTableMap: Map[String, MTable] = {
    val tableList = MTable.getTables.list(session)
    val tableMap = tableList.map { t => (t.name.name, t)}.toMap
    tableMap
  }

}