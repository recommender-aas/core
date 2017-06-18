package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.service.Util

import scala.concurrent.Future

case class Schema(
                   schemaId: java.util.UUID,
                   jsonSchema: Map[String, Any])

/**
  * Cassandra representation of the Schemas table
  */
class SchemaModel extends CassandraTable[ConcreteSchemaModel, Schema] {

  override def tableName: String = "schemas"

  object schemaId extends UUIDColumn(this) with PartitionKey

  object jsonSchema extends StringColumn(this)

  override def fromRow(r: Row): Schema = Schema(schemaId(r), Util.convertJson(jsonSchema(r)))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteSchemaModel extends SchemaModel with RootConnector {

  def getAll: Future[List[Schema]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getOne(schemaId: UUID): Future[Option[Schema]] = {
    select
      .where(_.schemaId eqs schemaId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .one
  }

  def save(schema: Schema): Unit = {
    insert
      .value(_.schemaId, schema.schemaId)
      .value(_.jsonSchema, Util.schemaToString(schema.jsonSchema))
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }

}
