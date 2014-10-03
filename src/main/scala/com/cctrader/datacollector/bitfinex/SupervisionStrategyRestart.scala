package com.cctrader.datacollector.bitfinex

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import scala.concurrent.duration._

/**
 *
 */
class SupervisionStrategyRestart extends akka.actor.SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = {
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: ArithmeticException => Restart
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Restart
      case _: Exception => Restart
    }
  }
}
