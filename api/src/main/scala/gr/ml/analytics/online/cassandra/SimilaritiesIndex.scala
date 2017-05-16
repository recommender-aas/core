package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.Future


case class SimilarityIndex (
                        pairId: String,
                        similarity: Double
                      )

class SimilaritiesIndexTable extends CassandraTable[SimilaritiesIndex, SimilarityIndex] {

  override def tableName: String = "similarity_index_table"

  object pairId extends StringColumn(this) with PartitionKey
  object similarity extends DoubleColumn(this)


  override def fromRow(row: Row): SimilarityIndex = {
    SimilarityIndex(
      pairId(row),
      similarity(row)
    )
  }
}

abstract class SimilaritiesIndex extends SimilaritiesIndexTable with RootConnector {

  def store(similarityIndex: SimilarityIndex): Future[ResultSet] = {
    insert.value(_.pairId, similarityIndex.pairId)
      .value(_.similarity, similarityIndex.similarity)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def updateSimilarity(similarityIndex: SimilarityIndex): Future[ResultSet] = {
    update
      .where(_.pairId eqs similarityIndex.pairId)
      .modify(_.similarity setTo similarityIndex.similarity)
      .future()
  }

  def getById(pairId: String): Future[Option[SimilarityIndex]] = {
    select.where(_.pairId eqs pairId).one()
  }
}