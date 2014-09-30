package com.cctrader.datacollector.bitfinex

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.{StaticQuery => Q}

/**
 * Writes ticks to the database and create (and write to the database) granularity's.
 */
class DBWriter(inSession: Session, resetGranularitys: Boolean) {

  implicit val session = inSession

  val tickTable = TableQuery[TickTable]

  var isLive = false

  val list: List[TickDataPoint] = tickTable.sortBy(_.id).list
  val iterator = list.iterator
  var tickDataPoint = iterator.next()

  var minTimestamp: Int = 0

  val tableMap = Map(
    //"bitfinex_btc_usd_1min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_1min")),
    //"bitfinex_btc_usd_2min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_2min")),
    //"bitfinex_btc_usd_5min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_5min")),
    //"bitfinex_btc_usd_10min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_10min")),
    //"bitfinex_btc_usd_15min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_15min")),
    //"bitfinex_btc_usd_30min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_30min")),
    "bitfinex_btc_usd_1hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_1hour")),
    "bitfinex_btc_usd_2hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_2hour")),
    "bitfinex_btc_usd_6hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_6hour")),
    "bitfinex_btc_usd_12hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_12hour")),
    "bitfinex_btc_usd_day" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitfinex_btc_usd_day"))
  )

  def lastRow(table: TableQuery[InstrumentTable]): DataPoint = {
      val idOfMax = table.map(_.id).max
      val firstOption = table.filter(_.id === idOfMax).firstOption
      firstOption.get
  }

  def lastTickBefore(timestamp: Int) = {
    val idOfLastProcessedTick = tickTable.filter(x => x.timestamp <= timestamp).map(_.id).max
    val lastProcessedTick = tickTable.filter(_.id === idOfLastProcessedTick).firstOption
    lastProcessedTick.get
  }

  val tableRows = {
    if (!resetGranularitys) {
      val lastTimestamp_1hour = lastRow(tableMap.get("bitfinex_btc_usd_1hour").get).timestamp
      val lastTimestamp_2hour = lastRow(tableMap.get("bitfinex_btc_usd_2hour").get).timestamp
      val lastTimestamp_6hour = lastRow(tableMap.get("bitfinex_btc_usd_6hour").get).timestamp
      val lastTimestamp_12hour = lastRow(tableMap.get("bitfinex_btc_usd_12hour").get).timestamp
      val lastTimestamp_day = lastRow(tableMap.get("bitfinex_btc_usd_day").get).timestamp

      minTimestamp = Math.min(Math.min(lastTimestamp_1hour, lastTimestamp_2hour), Math.min(lastTimestamp_6hour, Math.min(lastTimestamp_day, lastTimestamp_12hour)))

      Map(
        //"bitfinex_btc_usd_1min" -> NextRow(60, tickDataPoint),
        //"bitfinex_btc_usd_2min" -> NextRow(120, tickDataPoint),
        //"bitfinex_btc_usd_5min" -> NextRow(300, tickDataPoint),
        //"bitfinex_btc_usd_10min" -> NextRow(600, tickDataPoint),
        //"bitfinex_btc_usd_15min" -> NextRow(900, tickDataPoint),
        //"bitfinex_btc_usd_30min" -> NextRow(1800, tickDataPoint),
        "bitfinex_btc_usd_1hour" -> NextRow(3600, lastTickBefore(lastTimestamp_1hour)),
        "bitfinex_btc_usd_2hour" -> NextRow(7200, lastTickBefore(lastTimestamp_2hour)),
        "bitfinex_btc_usd_6hour" -> NextRow(21600, lastTickBefore(lastTimestamp_6hour)),
        "bitfinex_btc_usd_12hour" -> NextRow(43200, lastTickBefore(lastTimestamp_12hour)),
        "bitfinex_btc_usd_day" -> NextRow(86400, lastTickBefore(lastTimestamp_day))
      )
    }
    else {
      Map(
        //"bitfinex_btc_usd_1min" -> NextRow(60, tickDataPoint),
        //"bitfinex_btc_usd_2min" -> NextRow(120, tickDataPoint),
        //"bitfinex_btc_usd_5min" -> NextRow(300, tickDataPoint),
        //"bitfinex_btc_usd_10min" -> NextRow(600, tickDataPoint),
        //"bitfinex_btc_usd_15min" -> NextRow(900, tickDataPoint),
        //"bitfinex_btc_usd_30min" -> NextRow(1800, tickDataPoint),
        "bitfinex_btc_usd_1hour" -> NextRow(3600, tickDataPoint),
        "bitfinex_btc_usd_2hour" -> NextRow(7200, tickDataPoint),
        "bitfinex_btc_usd_6hour" -> NextRow(21600, tickDataPoint),
        "bitfinex_btc_usd_12hour" -> NextRow(43200, tickDataPoint),
        "bitfinex_btc_usd_day" -> NextRow(86400, tickDataPoint)
      )
    }
  }

  if (resetGranularitys) {
    println("Creating new data granularity-tables - Start")
    // drop all tables if exists and create new once.
    tableMap.foreach(x => {
      if (makeTableMap.contains(x._1.toString)) {
        x._2.ddl.drop
      }
      x._2.ddl.create
    })
    //add all ticks
    while (iterator.hasNext) {
      tickDataPoint = iterator.next()
      granulateTick(tickDataPoint)
    }
  }
  else {
    println("Creating new data granularity-tables - Start")
    val allNewRows = tickTable.filter(x => x.timestamp >= minTimestamp).sortBy(_.id)
    val allNewRowsList: List[TickDataPoint] = allNewRows.list
    val allNewRowsIterator = allNewRowsList.iterator

    while (allNewRowsIterator.hasNext) {
      tickDataPoint = allNewRowsIterator.next()
      granulateTick(tickDataPoint)
    }
  }

  def newTick(tickDataPoint: TickDataPoint) {
    //add the tick to the tick database
    println("TICK: id:" + tickDataPoint.id + ", sourceId:" + tickDataPoint.sourceId + ", unixTimestamp:" + tickDataPoint.timestamp + ", price:" + tickDataPoint.price + ", amount" + tickDataPoint.amount)
    tickTable += tickDataPoint
    granulateTick(tickDataPoint)
  }

  def granulateTick(tickDataPoint: TickDataPoint) {
    tableMap.foreach(x => {
      val granularity = x._1
      val table = x._2
      val row = tableRows(granularity)
      if(row.lastTimestamp <= tickDataPoint.timestamp) {
        while (row.endTimestamp < tickDataPoint.timestamp) {
          table += row.thisRow
          if (isLive) {
            // notify trading systems that a new dataPoint is added with id
            println("NOTIFY " + x._1.toString + " , '" + table.list.last.id.get + "'")
            Q.updateNA("NOTIFY " + x._1.toString + " , '" + table.list.last.id.get + "'").execute
          }
          row.updateNoTickNextRow()
        }
        row.addTick(tickDataPoint)
      }
    })
  }

  def getEndTime: Int = {
    val idOfMax = tickTable.map(_.id).max
    val firstOption = tickTable.filter(_.id === idOfMax).firstOption
    firstOption.get.timestamp
  }


  def javaNewTick(sourceId: Long, unixTimestamp: Int, price: Double, amount: Double) {
    newTick(TickDataPoint(None, Some(sourceId), unixTimestamp, price, amount))

  }

  println("Creating granularity-tables - Finished")

  def makeTableMap: Map[String, MTable] = {
    val tableList = MTable.getTables.list(session)
    val tableMap = tableList.map { t => (t.name.name, t)}.toMap
    tableMap
  }

  case class NextRow(intervalSec: Int, firstTick: TickDataPoint) {
    var open = firstTick.price
    var high = firstTick.price
    var low = firstTick.price
    var volume = firstTick.amount
    var close = firstTick.price
    var lastTimestamp = firstTick.timestamp
    var endTimestamp = lastTimestamp + intervalSec
    var lastSourceId = firstTick.sourceId

    def reinitialize(tick: TickDataPoint) {
      open = tick.price
      high = tick.price
      low = tick.price
      volume = tick.amount
      close = tick.price
      endTimestamp = tick.timestamp + intervalSec
      lastSourceId = tick.sourceId
    }

    def addTick(tick: TickDataPoint): Unit = {
      lastSourceId = tick.sourceId
      if (volume == 0) {
        open = tick.price
        high = tick.price
        low = tick.price
        volume = tick.amount
        close = tick.price
      }
      else {
        if (tick.price > high) {
          high = tick.price
        }
        else if (tick.price < low) {
          low = tick.price
        }
        volume = volume + tick.amount
        close = tick.price
      }
    }

    def updateNoTickNextRow() = {
      reinitialize(TickDataPoint(None, lastSourceId, endTimestamp, close, 0))
    }

    def thisRow: DataPoint = {
      DataPoint(None, lastSourceId, endTimestamp, open, close, low, high, volume)
    }
  }


}

