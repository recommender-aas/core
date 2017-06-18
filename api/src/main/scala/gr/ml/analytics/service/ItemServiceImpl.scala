package gr.ml.analytics.service

import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraStorage, Schema}

import scala.concurrent.Future
import scala.util.parsing.json.JSONObject
import scala.concurrent.ExecutionContext.Implicits.global

class ItemServiceImpl(val inputDatabase: CassandraStorage) extends ItemService with LazyLogging {

  /**
    * @inheritdoc
    */
  override def get(schemaId: UUID, itemId: Int): Future[Option[Map[String, Any]]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)
        val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schemaMap)

        // retrieve content in json format
        val tableName = Util.itemsTableName(schemaId)
        val query = s"SELECT JSON * FROM ${inputDatabase.ratingModel.keySpace}.$tableName WHERE $idName = $itemId"
        val res = inputDatabase.connector.session.execute(query).one()

        if (res != null) {
          val item: Map[String, Any] = Util.convertJson(res.get("[json]", classOf[String]))
          Some(item)
        } else {
          None
        }

      case None => None
    }
  }

  /**
    * @inheritdoc
    */
  override def get(schemaId: UUID, itemIds: List[Int]): Future[List[Option[Map[String, Any]]]] = {
    Future.sequence(itemIds.map(itemId => get(schemaId, itemId)))
  }

  /**
    * @inheritdoc
    */
  override def save(schemaId: UUID, item: Map[String, Any]): Future[Option[Int]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema: Schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)
        val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schemaMap)


        // TODO implement proper mechanism to escape characters which have special meaning for Cassandra
        val json = JSONObject(item).toString().replace("'", "")
        val tableName = Util.itemsTableName(schemaId)

        println(json)
        val query = s"INSERT INTO ${inputDatabase.ratingModel.keySpace}.$tableName JSON '$json'"
        val res = inputDatabase.connector.session.execute(query).wasApplied()

        logger.info(s"Creating item: '$query' Result: $res")

        Some(item(idName.toLowerCase()).asInstanceOf[Integer].intValue())
      case None =>
        None
    }
  }
}
