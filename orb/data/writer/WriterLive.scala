package orb.data.writer

import orb.api.http.data.*
import orb.catalog.{Catalog, Service as catalogService}
import orb.core.*
import orb.cql.decode.*
import orb.cql.decode.given
import orb.cql.*
import orb.cql
import orb.cql.{Insert, Service as CqlService, *}
import orb.cql.given
import orb.data.identity.{Service as IdentityService, *}
import orb.sugar.*
import orb.tree.StorageType.*
import orb.tree.{GroupedStorageLocation, RootNode, Service as treeService}
import zio.*
import zio.logging.Logging
import zio.clock.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}
import scala.language.postfixOps
import com.datastax.driver.core.ResultSet
import io.circe.*
import orb.compaction.{CompactionError, CompactionFiber}
import orb.counter.*
import zmx.metrics._
import orb.metrics._

import orb.counter.{Counter => OrbCounter, *}

class LiveWriter(theCatalog: catalogService, theTree: treeService, db: CqlService, theIdentity: IdentityService, logger: zio.logging.Logger[String], clock: zio.clock.Clock.Service, theCompaction: orb.compaction.Service) extends orb.data.writer.Service {


  def parseFromWriteToWriteRequest(tree: RootNode, enums: Seq[EnumRef], write: Write): Either[io.circe.Error, WriteRequest] = {
    given RootNode = tree
    given Seq[EnumRef] = enums

    parser.decode[WriteRequestAttributes](write.attributes.toString).map{attrs => {
      WriteRequest(
        write.layer,
        write.identities.map(id => RequestIdentity(id.namespace, id.key)).toList,
        WriteRequestOptions(write.options.resilientWrite),
        attrs
      )
    }}
  }


  override def write(layer: LayerRef, writeRequest: WriteRequest): Task[(WriteResult, CompactionFiber)] = (for {
    eid              <- eid(layer.entity, writeRequest)
    (errors, fdList) = writeRequest.attributes.parsingResults.partitionMap(identity)
    res              <- if (writeRequest.options.resilientWrite) resilientWrite(eid, layer, fdList, errors) else atomicWrite(eid, layer, fdList, errors)
    fib              <- theCompaction.compact(eid, layer.entity).forkDaemon
  } yield res -> fib)
    @@ OrbMetricAspect("write", tags = Seq("action" -> "write")).countRps
    @@ OrbMetricAspect("write_duration", tags = Seq("action" -> "write_duration")).countDuration

  protected def eid(entity: EntityRef, writeRequest: WriteRequest): ZIO[Any, ServiceError, UUID] = for  {
    identities <- ZIO.foreachPar(writeRequest.identities)(identity => for {
                      ns <- theCatalog.ensureNamespace(entity, identity.namespace)
                  } yield Identity(ns, identity.key)
    )
    eid              <- theIdentity.getOrCreateIdentity(entity, identities)
  } yield eid

  type IdentityId = String

  protected def prepareFieldsAndValuesForInsert(fd: FieldData): Map[String, Any] =
    Map(
      fd.storageLocation.get.column -> fd.value,
      fd.storageLocation.get.column + "_ts" -> CurrentTSCqlExpr()
    )
      ++
      fd.parentIdsAndKeyValues.flatMap((key, value) => Map(s"parent$key" -> value))
      ++
      fd.collectionColumnKeyAndValue


  protected def resilientWrite(eid: UUID, layerRef: LayerRef, fdList: Seq[FieldData], errors: Seq[String]): Task[WriteResult] = for {
    writeError <- write(eid, layerRef, fdList)
  } yield {
    val errorSeq = errors :+ writeError
    WriteResult(Option(eid), WriteStatus.Success, errorSeq)
  }

  protected def preparedInsertSeq(fdList: Seq[FieldData], additionalFields: Map[String, Any]): Seq[Insert] = {
    fdList
      .groupBy(_.storageLocation.getOrElse(throw new Exception("fdList" + fdList.toString() + "has no nodeStorageLocation")).table)
      .map((table, seqOfFieldData) =>
        table.storageType match {
          case Attrs =>
            Seq(Insert(table.name.asCqlTable, Seq(seqOfFieldData.map(listData => prepareFieldsAndValuesForInsert(listData)).flatten.toMap ++ additionalFields)))
          case Collection(_) =>
            seqOfFieldData
              .groupBy(fd => (fd.collectionColumnKeyAndValue -> fd.parentIdsAndKeyValues))
              .map((keysAndParentIds, collectionseqFieldData) => {
                Insert(table.name.asCqlTable, Seq(collectionseqFieldData.map(listData => prepareFieldsAndValuesForInsert(listData)).flatten.toMap ++ additionalFields))
              }).toSeq
        }
      )
      .toSeq.flatten
  }

  protected def atomicWrite(eid: UUID, layerRef: LayerRef, fdList: Seq[FieldData], errors: Seq[String]): Task[WriteResult] = {
    if (errors.nonEmpty) Task(WriteResult(None, WriteStatus.Success, errors))
    else {
      for {
        error <- write(eid, layerRef, fdList)
      } yield WriteResult(Option(eid), status = if (error.isEmpty) WriteStatus.Success else WriteStatus.Error, Seq(error))
    }
  }

  //toDo ????? if is empty else Task ?
  def write(eid: UUID, layerRef: LayerRef, fdSeq: Seq[FieldData]): Task[String] = {
    if (fdSeq.nonEmpty) {
      val inserts = preparedInsertSeq(fdSeq, Map("eid" -> eid, "layer_cid" -> layerRef.cid, "ts" -> CurrentTSCqlExpr()))
      val valuesForCounter = OrbCounter.prepareValuesForCounter(inserts)

      for {
        writeResult <- db.insertMulti(inserts)
        res         <- insertCounter(layerRef.entity, valuesForCounter, eid)
        _           <- if (!res.wasApplied()) updateCounter(layerRef.entity, valuesForCounter, eid) else ZIO.unit
        _           <- logger.info(inserts.toString) *> logger.info(valuesForCounter.toString)
      } yield writeResult.toString
    }
    else Task("")
  }

  protected def updateCounter(entity: EntityRef, values: CounterValues, eid: UUID): CqlTask[ResultSet] =
    val update = db.buildUpdateStatement(db.h.attrsCounters(entity).name, values.map((tableName, rows) => tableName -> CqlExprRaw(s"${tableName} + $rows")), cql"eid = $eid")
    db.execute(update)

  protected def insertCounter(entity: EntityRef, values: CounterValues, eid: UUID): CqlTask[ResultSet] =
    val insert = db.buildUpdateStatement(db.h.attrsCounters(entity).name, values, cql"eid = $eid", "NOT EXISTS")
    db.execute(insert)
}


