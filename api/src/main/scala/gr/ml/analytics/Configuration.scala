package gr.ml.analytics

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

object Configuration {

  val config: Config = ConfigFactory.load("application.conf")

  val serviceListenerInterface: String = config.getString("service.listener.iface")
  val serviceListenerPort: Int = config.getInt("service.listener.port")

  val swaggerRestHost: String = config.getString("swagger.rest.host")
  val swaggerRestPort: Int = config.getInt("swagger.rest.port")

  val onlineLearningEnable: Boolean = config.getBoolean("online.learning.enable")

  val cassandraHosts: List[String] = config.getStringList("cassandra.hosts").toList
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraUsername: String = config.getString("cassandra.username")
  val cassandraPassword: String = config.getString("cassandra.password")
}
