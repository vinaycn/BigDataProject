name := """StayRecommendationApp"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test
)


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
libraryDependencies += "com.typesafe.play" %% "play-slick" % "1.1.0"
libraryDependencies += "com.typesafe.play" %% "play-slick-evolutions" % "1.1.0"

libraryDependencies += evolutions

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "com.adrianhurt" %% "play-bootstrap" % "1.1-P25-B3"
)