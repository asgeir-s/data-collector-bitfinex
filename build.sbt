import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

test in assembly := {}

organization  := "com.cctrader"

name := "BitfinexCollector"

version := "1.0"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  Seq(
    "com.typesafe.akka"       %%  "akka-actor"                  % akkaV,
    "com.typesafe.slick"      %%  "slick"                       % "2.1.0",
    "org.slf4j"               %   "slf4j-nop"                   % "1.6.4",
    "org.postgresql"          %   "postgresql"                  % "9.3-1101-jdbc41",
    "org.json4s"              %%  "json4s-native"               % "3.2.9"
  )
}
