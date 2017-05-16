package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.Future


case class Similarity (
                          itemId: String,
                          anotherItemId: String,
                          similarity: Double
                          )

class SimilaritiesTable extends CassandraTable[Similarities, Similarity] {

  override def tableName: String = "similarities_table"

  object itemId extends StringColumn(this) with PartitionKey
  object similarity extends DoubleColumn(this) with ClusteringOrder with Descending
  object anotherItemId extends StringColumn(this) with ClusteringOrder with Ascending


  override def fromRow(row: Row): Similarity = {
    Similarity(
      itemId(row),
      anotherItemId(row),
      similarity(row)
    )
  }
}

abstract class Similarities extends SimilaritiesTable with RootConnector {

  def store(similarity: Similarity): Future[ResultSet] = {
    insert.value(_.itemId, similarity.itemId)
          .value(_.anotherItemId, similarity.anotherItemId)
          .value(_.similarity, similarity.similarity)
          .consistencyLevel_=(ConsistencyLevel.ALL)
          .future()
  }

  def updateSimilarity(similarity: Similarity): Future[ResultSet] = {
    update
      .where(_.itemId eqs similarity.itemId)
      .and(_.anotherItemId eqs similarity.anotherItemId)
      .modify(_.similarity setTo similarity.similarity)
      .future()
  }

  def deleteRow(similarity: Similarity): Future[ResultSet] = {
    delete.where(_.itemId eqs similarity.itemId)
            .and(_.similarity eqs similarity.similarity)
            .and(_.anotherItemId eqs similarity.anotherItemId)
            .future()
  }

  def getById(id: String, limit: Int = 10): Future[Seq[Similarity]] = {
    select.where(_.itemId eqs id).limit(limit).fetch()
  }
}