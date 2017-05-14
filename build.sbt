import sbt.Keys.{libraryDependencies, _}
import sbt.Resolver

val commonSettings = Seq(
  organization := "gr.ml.analytics",
  /* Get short version of the last commit id */
  //version := s"${Process("git log --pretty=format:'%h' -n 1").lines.head}",
  version := "0.1",
  scalaVersion := "2.11.8",
  javacOptions ++= Seq("-encoding", "UTF-8")
)

/**
  * Setting to generate a class with module-specific properties during compilation
  */
val buildInfoSettings = Seq(
  sourceGenerators in Compile <+= (sourceManaged in Compile, version, name) map { (d, v, n) =>
    val file = d / "info.scala"
    val pkg = "gr.ml.analytics"
    IO.write(file,
      s"""package $pkg
         |class BuildInfo {
         |  val info = Map[String, String](
         |    "name" -> "%s",
         |    "version" -> "%s"
         |    )
         |}
         |""".stripMargin.format(n, v))
    Seq(file)
  }
)

// dependency versions

val akkaVersion = "2.4.17"
val akkaHttpVersion = "10.0.5"
val phantomVersion = "2.7.6"
val sparkVersion = "2.0.1"

// module structure configuration

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "recommender-saas",
    description :=
      """
        |Root project of recommender as a service software which aggregates
        |set of modules related to the project.
      """.stripMargin
  )
  .aggregate(service, api, common, demo)


lazy val common = project.in(file("common"))
  .settings(commonSettings: _*)
  .settings(
    name := "recommender-saas-common",
    description :=
      """
        |Common utilities and services shared across modules inside recommender as a service software.
      """.stripMargin
  )

lazy val api = project.in(file("api"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommender-saas-api",
    description :=
      """
        |REST API for recommender as a service software
      """.stripMargin
  )
  .settings(

    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
    ),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.megard" %% "akka-http-cors" % "0.1.11",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    ),
    // swagger support
    libraryDependencies += "co.pragmati" %% "swagger-ui-akka-http" % "1.0.0",
//    libraryDependencies += "io.swagger" % "swagger-jaxrs" % "1.5.13",
    libraryDependencies += "com.github.swagger-akka-http" %% "swagger-akka-http" % "0.9.1",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.14.0",
    libraryDependencies += "com.outworkers" %% "phantom-dsl" % phantomVersion,
    libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.1",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    libraryDependencies += "net.debasishg" %% "redisclient" % "3.4",
    libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.5.1",

    libraryDependencies += "org.cassandraunit" % "cassandra-unit" % "3.1.3.2" % "test",

    libraryDependencies += "com.outworkers" %% "util-testing" % "0.30.1" % "test",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
    libraryDependencies += "org.specs2" %% "specs2-core" % "3.8.9" % "test"
  )
  .dependsOn(common)

lazy val service = project.in(file("service"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommender-saas-service",
    description :=
      """
        |Recommender service where all magic happens
      """.stripMargin
  )
  .settings(
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Spray Repository" at "http://repo.spray.io",
      "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    ),
    libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11",
    libraryDependencies += "com.typesafe" % "config" % "1.3.1",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "org.specs2" %% "specs2" % "2.3.13" % "test",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
    libraryDependencies += "net.debasishg" %% "redisclient" % "3.4"
  )
  .dependsOn(common)


/*
This module does not actually belong to project API, it just shows up how
recommender SaaS can be used with MovieLens dataset https://grouplens.org/datasets/movielens/1m/
 */
lazy val demo = project.in(file("movielens_demo"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(

    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
    ),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    ),
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
  )

// Add any command aliases that may be useful as shortcuts
addCommandAlias("cct", ";clean;compile;test")
