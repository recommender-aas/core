package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.Future

case class PairCount(
                      pairId: String,
                      count: Double
                    )


class PairCountsTable extends CassandraTable[PairCounts, PairCount] {

  override def tableName: String = "pair_counts_table"

  object pairId extends StringColumn(this) with PartitionKey

  object count extends DoubleColumn(this)

  override def fromRow(row: Row): PairCount = {
    PairCount(
      pairId(row),
      count(row)
    )
  }
}

abstract class PairCounts extends PairCountsTable with RootConnector {

  def store(pairCount: PairCount): Future[ResultSet] = {
    insert.value(_.pairId, pairCount.pairId).value(_.count, pairCount.count)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(id: String): Future[Option[PairCount]] = {
    select.where(_.pairId eqs id)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .one()
  }

  def incrementCount(pairId: String, deltaWeight: Double): Future[_] = {
    getById(pairId).flatMap {
      case Some(pairCount) =>
        update.where(_.pairId eqs pairId).modify(_.count setTo (deltaWeight + pairCount.count)).future()
      case None => store(PairCount(pairId, deltaWeight))
    }
  }

}