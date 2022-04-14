package orb.compaction

import com.datastax.driver.core.ResultSet
import zio.*
import orb.cql.{Service as CqlService, given, _}
import orb.tree.Service as TreeService
import orb.catalog.{CatalogError, ClientError, InternalError, Service as CatalogService}
import orb.core.EntityRef
import orb.cql.decode.{given, _}
import orb.sugar.*
import java.util.{Date, UUID}
import orb.conf.{Root, Service as ConfService}
import orb.data.identity.{MergedEids, Service as IdentityService}
import orb.counter.{Counter => OrbCounter, *}
import zmx.metrics._
import orb.metrics._

import collection.JavaConverters.*

def CompactionTask[R,E <: Throwable,A](zio:ZIO[R,E,A]):ZIO[R,CompactionError,A] = zio.refineOrDie {
  case c: CompactionError => c
  case e: Throwable => orb.compaction.InternalError(e.getMessage)
}

case class CompactionLive(threshold: Int, db: CqlService, logger: zio.logging.Logger[String]) extends orb.compaction.Service {

  def compact(eid: UUID, entity: EntityRef): CompactionTask[Unit] = CompactionTask { for {
    unmergedEids <- MergedEids(db).collectMergedEids(entity, eid)
    tablesWithNumberOfRowsOpt <- db.selectOne[CqlRecordWithNulls](cql"SELECT * FROM ${db.h.attrsCounters(entity)} WHERE eid = $eid")
    tablesToCompact = if (unmergedEids.isEmpty) filterTablesToCompact(tablesWithNumberOfRowsOpt) else allTablesToCompact(tablesWithNumberOfRowsOpt)
    inserts <- prepareInsers(eid, unmergedEids, entity, tablesToCompact)
      _     <- logger.info("Doing compaction..")
    _       <- logger.info(tablesToCompact.toString())
    _       <- ZIO.when(!inserts.isEmpty) {
                      db.execute(prepareTx(inserts, entity, eid, unmergedEids)).as(()).catchAll(e => ZIO.succeed(e.toString))
                 }
    _ <- ZIO.when(!unmergedEids.isEmpty) { merge(unmergedEids, entity) }
    } yield ()
  }

  protected def merge(unmergedUuids: Seq[UUID], entity: EntityRef): Task[ResultSet] = db.execute(CqlTransaction(prepareMergeCqlStatements(entity, unmergedUuids)))


  protected def prepareMergeCqlStatements(entity: EntityRef, eids: Seq[UUID]): Seq[CqlStatement] = eids.flatMap(eid =>
    Seq(
      cql"DELETE FROM ${db.h.attrsCounters(entity)} WHERE eid = $eid;",
      cql"DELETE FROM ${db.h.mergedEids(entity)} WHERE from_eid = $eid;",
    )
  )

  protected def prepareTx(inserts: Seq[Insert], entity: EntityRef, eid: UUID, unmergedEids: Seq[UUID]): CqlTransaction = {
    val countUpdate = prepareCounterUpdate(db.h.attrsCounters(entity), OrbCounter.prepareValuesForCounter(inserts), eid)
    val deletion    = prepareDeleteFromAttrsTable(inserts, eid +: unmergedEids)
    db.buildInsertMultiTransaction(inserts, ifExistsError = true).add(countUpdate).add(deletion)
  }

  protected def prepareInsers(eid: UUID, unmergedEids: Seq[UUID], entity: EntityRef, tablesToCompact: CqlRecordWithNulls): ZIO[Any, CqlException, Seq[Insert]] = for {
    inserts <- ZIO.foreachPar(tablesToCompact.toSeq)((tableName, _) => { for {
        res <- blockCompaction(entity, tableName, eid)
        inserts <- if (res.wasApplied()) for {
          rows <- rowsToCompact(tableName, eid +: unmergedEids)
          merdedRecords = rows
            // layer cid is part of primary key
            .groupBy(cqlRecordWithNulls => filterFieldsToGroup(cqlRecordWithNulls).map(_._2.get))
            .flatMap {
              (fieldsToGroup, seq) =>
                if (seq.size > 1) Map(fieldsToGroup -> seq.foldRight(Map.empty[String, Option[Any]]) { (el, acc) => if (acc.isEmpty) el else merge(acc, el) })
                else Map.empty
            }

          inserts = merdedRecords.map {
            (_, values) => {
              Insert(tableName.asCqlTable, Seq(prepareValuesForInsert(values, eid)))
            }
          }.toSeq
        } yield inserts
        else ZIO.succeed(Seq.empty)
      } yield inserts
    } @@ aspCountAll(Seq(("action" -> "compact"), ("table" -> tableName))) )
  } yield inserts.flatten

  def blockCompaction(entity: EntityRef, tableName: String, eid: UUID) = db.execute(cql"INSERT INTO ${db.h.attrsCompactions(entity)} (eid, table_name, ts) VALUES ($eid, $tableName, currenttimestamp()) IF NOT EXISTS USING ttl 10")

  protected def prepareValuesForInsert(values: Map[String, Option[Any]], eid: UUID):Map[String, Any] = values.collect {
    case (fieldName, Some(value)) => fieldName -> transformListToJava(value)
  } ++ Map("eid" -> eid, "ts" -> CurrentTSCqlExpr())

  protected def filterTablesToCompact(tablesAndNumberOfRowsOpt: Option[CqlRecordWithNulls]): CqlRecordWithNulls = tablesAndNumberOfRowsOpt.map {
    _.-("eid").filter(_._2 match {
      case Some(count) => count.asInstanceOf[Long] >= threshold
      case _           => false
    })
  } match {
    case Some(map) => map
    case _         => Map.empty
  }

  protected def allTablesToCompact(tablesAndNumberOfRowsOpt: Option[CqlRecordWithNulls]): CqlRecordWithNulls = tablesAndNumberOfRowsOpt.map (_.-("eid")) match {
    case Some(map) => map
    case _         => Map.empty
  }

  protected def prepareCounterUpdate(counterTable: CqlTable, counterValues: CounterValues, eid: UUID): CqlStatement = {
    val values = counterValues.map((tableName, rows) => tableName -> CqlExprRaw(s"$tableName - ${rows - 1}"))
    db.buildUpdateStatement(counterTable.name, values, cql"eid = $eid")
  }


  protected def prepareDeleteFromAttrsTable(inserts: Seq[Insert], eids: Seq[UUID]): Seq[CqlStatement] =
    eids.map(eid => inserts.map(insert => cql"DELETE FROM ${insert.table} WHERE eid = $eid AND ts < ${CurrentTSCqlExpr()};")).flatten

  protected def rowsToCompact(tableName: String, eids: Seq[UUID]): CqlTask[Seq[CqlRecordWithNulls]] = for {
       rows <- ZIO.foreachPar(eids) (eid => db.selectAll[CqlRecordWithNulls](cql"SELECT * FROM ${tableName.asCqlTable} WHERE eid = $eid") )
     } yield rows.flatten

  // toDO maybe better to check field type from the tree
  protected def transformListToJava(value: Any):Any = if (value.isInstanceOf[List[Any]]) value.asInstanceOf[List[Any]].asJava else value

  protected def merge(el: CqlRecordWithNulls, acc: CqlRecordWithNulls):CqlRecordWithNulls = {
    val elValues = filterValues(el)
    val elDates  = filterDates(el)

    elValues.flatMap {
        (cqlRecord: (String, Option[Any])) => {
          val fName = cqlRecord._1

          for {
            accValueOpt <- acc.get(fName)
            accDateOpt <- acc.get(tsByFieldName(fName))
          } yield {
            val (resDate, resValue) = compareByDate(accDateOpt.asInstanceOf[Option[Date]]-> accValueOpt, elDates.get(tsByFieldName(fName)).get.asInstanceOf[Option[Date]] -> elValues.get(fName).get)
            Map(fName -> resValue, tsByFieldName(fName) -> resDate)
          }
        }
      }.flatten.toMap ++ filterFieldsToGroup(el)
  }

  protected def tsByFieldName(name:String):String = name + "_ts"

  protected def compareByDate(t1: (Option[Date], Option[Any]), t2: (Option[Date], Option[Any])): (Option[Date], Option[Any]) = (t1, t2) match {
    case ((Some(date1), v1), (Some(date2), v2)) => if date1.after(date2) then Some(date1) -> v1 else Some(date2) -> v2
    case ((Some(date1), v1), (None, _))         => Some(date1) -> v1
    case ((None, _), (Some(date2), v2))         => Some(date2) -> v2
    case _                                      => (None, None)
  }

  protected def filterValues(record: CqlRecordWithNulls): CqlRecordWithNulls = record.filter(t2 => t2._1.matches("f\\d+$"))

  protected def filterDates(record: CqlRecordWithNulls): CqlRecordWithNulls = record.filter(t2 => t2._1.matches("f\\d+_ts"))

  protected def filterFieldsToGroup(record: CqlRecordWithNulls): CqlRecordWithNulls = record
    .filter(t2 => !t2._1.matches("f\\d+$") && !t2._1.matches("f\\d+_ts"))
    .--(Seq("ts", "eid"))
}
