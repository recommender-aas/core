package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraCache, InputDatabase}
import gr.ml.analytics.domain.{Item, Schema}

import scala.concurrent.Future
import scala.util.parsing.json.JSONObject
import scala.concurrent.ExecutionContext.Implicits.global

class ItemServiceImpl(val inputDatabase: InputDatabase, val cassandraCache: CassandraCache) extends ItemService with LazyLogging {

//  private lazy val notRatedItemsWithFeaturesModel = inputDatabase.notRatedItemsWithFeaturesModel

  /**
    * @inheritdoc
    */
  override def get(schemaId: Int, itemId: Int): Future[Option[Item]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)

        // retrieve content in json format
        val tableName = Util.itemsTableName(schemaId)
        val query = s"SELECT JSON * FROM ${inputDatabase.ratingModel.keySpace}.$tableName WHERE $idName = $itemId"
        val res = inputDatabase.connector.session.execute(query).one()

        if (res != null) {
          val item: Item = Util.convertJson(res.get("[json]", classOf[String]))
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
  override def get(schemaId: Int, itemIds: List[Int]): Future[List[Option[Item]]] = {
    Future.sequence(itemIds.map(itemId => get(schemaId, itemId)))
  }

  /**
    * @inheritdoc
    */
  override def save(schemaId: Int, item: Item): Future[Option[Int]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema: Schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)

        // TODO implement proper mechanism to escape characters which have special meaning for Cassandra
        val json = JSONObject(item).toString().replace("'", "")
        val tableName = Util.itemsTableName(schemaId)
        val tableNameDense = Util.itemsTableName(schemaId) + "_dense"
        val keyspace = inputDatabase.ratingModel.keySpace
        val query = s"INSERT INTO $keyspace.$tableName JSON '$json'"
        val res = inputDatabase.connector.session.execute(query).wasApplied()

        // inserting into dense items table
        val itemId = item.get(idName).get.toString.toInt


        val featureValues: List[Double] = Util.getFeaturesValues(schemaMap, item)
        val featuresString = "[" + featureValues.toArray.mkString(", ") + "]"

        val insertDenseItemQuery = s"INSERT INTO $keyspace.$tableNameDense (itemid, features) values ($itemId, $featuresString)"
        inputDatabase.connector.session.execute(insertDenseItemQuery)

        logger.info(s"Creating item: '$query' Result: $res")

        cassandraCache.invalidateItemIDs()

        val userIds = cassandraCache.getAllUserIDs()
        userIds.foreach(userId => {
          val addNotRatedItemWithFeaturesQuery = s"UPDATE $keyspace.not_rated_items_with_features " +
            s"set items = items + {$itemId : $featuresString} where userid = $userId";
          inputDatabase.connector.session.execute(addNotRatedItemWithFeaturesQuery)
//          notRatedItemsWithFeaturesModel.addNotRatedItem(userId, itemId, featureValues)
        })

        Some(item(idName.toLowerCase()).asInstanceOf[Integer].intValue())
      case None =>
        None
    }
  }
}
