import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

test in assembly := {}

organization  := "com.cctrader"

name := "BitfinexCollector"

version := "1.0"

scalaVersion  := "2.10.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "com.typesafe.akka"       %%  "akka-actor"                  % akkaV,
    "com.typesafe.slick"      %%  "slick"                       % "2.1.0",
    "org.slf4j"               %   "slf4j-nop"                   % "1.6.4",
    "org.postgresql"          %   "postgresql"                  % "9.3-1101-jdbc41",
    "io.spray"                %%  "spray-can"                   % sprayV,
    "io.spray"                %%  "spray-routing"               % sprayV,
    "io.spray"                %%  "spray-http"                  % sprayV,
    "io.spray"                %%  "spray-httpx"                 % sprayV,
    "io.spray"                %%  "spray-util"                  % sprayV,
    "io.spray"                %%  "spray-json"                  % "1.3.0",
    "io.spray"                %%  "spray-client"                % sprayV
  )
}
