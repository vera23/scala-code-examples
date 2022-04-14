package orb.data.identity

import orb.api.http.data.Identity
import orb.core.{EntityRef, NamespaceRef}
import orb.cql.{Service as CqlService, given, *}
import zio.{Schedule, Task, ZIO}
import orb.core.*
import orb.sugar.*
import zio.*
import orb.cql.decode.*
import orb.cql.decode.given
import orb.api.http.data.{Identity, RequestIdentity}
import zio.duration.*
import java.util.UUID
import orb.catalog.{Catalog, Service as CatalogService}
import zio.clock.*
import orb.data.identity.Service as IdentityService
import com.datastax.driver.core.exceptions.InvalidQueryException

import java.util.UUID
import scala.util.matching.Regex
import orb.metrics.OrbMetricAspect
import zio.zmx.metrics.MetricsSyntax

case class Merged(from: UUID, to: UUID)

class IdentityLive(db: CqlService, catalog: CatalogService, clock: zio.clock.Clock.Service,  logger: zio.logging.Logger[String]) extends IdentityService {

  override def ensureIdentity(entity: EntityRef, namespace: NamespaceRef, key: Identifier): IdentityRepoTask[IdentityRef] = IdentityRepoTask {
    for
      _           <- isValidIdentityKey(key) ensureOrFail ClientError(s"Identity key $key is not valid")
      opt         <- {
        db.selectOne[Tuple1[UUID]](cql"SELECT eid FROM ${db.h.nskeyEid(entity)} WHERE key = $key AND ns_cid = ${namespace.cid}")
          @@ OrbMetricAspect("ir_select_eid_duration", Seq("action" -> "ir_select_eid_duration")).countDuration
      }
      row         <- opt getOrFail ClientError(s"Could not find entity with identity [${namespace.name}:$key]")
      mergedEids <- {
        MergedEids(db).collectMergedEids(entity, row._1)
          @@ OrbMetricAspect("ir_collectMergedEids_duration", Seq("action" -> "ir_collectMergedEids_duration")).countDuration
      }
    yield IdentityRef(entity, row._1, namespace, key, mergedEids)
  } @@ OrbMetricAspect("ir_ensureIdentity_rps", Seq("action" -> "ir_ensureIdentity_rps")).countRps
    @@ OrbMetricAspect("ir_ensureIdentity_duration", Seq("action" -> "ir_ensureIdentity_duration")).countDuration

  override def findIdentities(entity: EntityRef, eid: UUID): IdentityRepoTask[Seq[IdentityRef]] = IdentityRepoTask {
    {
      for
        identityRows <- db.selectAll[(CID, Identifier)](cql"SELECT ns_cid, key FROM ${db.h.eidNskey(entity)} WHERE eid = ${eid}")
        identityRefs <- ZIO.foreachPar(identityRows) {
          (nsCid, key) => for {
            namespaceRef <- catalog.ensureNamespace(entity, nsCid)
            mergedEids <- MergedEids(db).collectMergedEids(entity, eid)
          } yield IdentityRef(entity, eid, namespaceRef, key, mergedEids)
          }
     //   collected <- Task.collectAll(identityRefs)
      yield identityRefs
    } @@ OrbMetricAspect("findIdentities", Seq("action" -> "findIdentities")).countRps
      @@ OrbMetricAspect("findIdentities_duration", Seq("action" -> "findIdentities_duration")).countDuration
  }

  override def getOrCreateIdentity(entity: EntityRef, identities: Seq[Identity], sleep: Int = 0): IdentityRepoTask[UUID] = IdentityRepoTask {( for {
      uuid          <- execute(entity, identities, sleep)
    } yield uuid).orElse(logger.error("trying..") *> execute(entity, identities, sleep))
    }

  override def getIdsPartitioned(entity: EntityRef,
                                 perPartitionLimit: Int = 10,
                                 scaleFactor:Int = 8,
                                 parallelTasks:Int = 16): IdentityRepoTask[Seq[String]] = IdentityRepoTask{
    val tasksPerTable = (4096 * scala.math.pow(2, (scaleFactor-6))).toInt
    val rangeSize = 65536 / tasksPerTable
    val lowBounds = scala.util.Random.shuffle(for i <- 0 until tasksPerTable yield i * rangeSize)
    val bounds = for lowBound <- lowBounds yield (lowBound, lowBound + rangeSize - 1)

    val phash = CqlExpr(s"partition_hash(eid)")

    def selectInBounds(lower: Int, upper: Int): Task[Seq[String]] =
      db.selectAll[Tuple1[String]](
        cql"""SELECT key
          FROM ${CqlExpr(db.h.defaultKeyspace)}.${CqlExpr(db.h.eidNskey(entity).name)}
          WHERE ${phash} >= ${lower} AND ${phash} <= ${upper}
          LIMIT ${perPartitionLimit}""").map(_.map(_._1))

    for
      idChunks <- ZIO.foreachParN(parallelTasks)(bounds)(selectInBounds.tupled(_))
    yield idChunks.reduce(_ ++ _)
  }

  def execute(entity: EntityRef, identities: Seq[Identity], sleep: Int = 0): Task[UUID] = for {
    identitiesAndUuids             <- eidsForIdentities(entity, identities)
    (oldIdentities, newIdentities) = divideOldAndNewIdentities(identitiesAndUuids)
    allIdentitiesWithSameUuid      <- allIdentitiesWithSameUUID(entity, groupByUuid(oldIdentities.map((identity, uuidOpt) => identity -> uuidOpt.get)))
    (uuid, cqlStatements)          <- prepareStatements(entity, allIdentitiesWithSameUuid, newIdentities, sleep)
    _                              <- ZIO.when(cqlStatements.size > 0)(db.execute(CqlTransaction(cqlStatements)))
      .flatMapError {
      case e: CqlException if identityHasConflict(e.getMessage, entity) => for {
        identities <- identitiesWithConflicts(entity, identities).catchAll(_ => ZIO.succeed(Map.empty))
        msg        = if(identities.nonEmpty) prepareMessage(identities) else e.getMessage
      } yield IdentityConflictError(msg, ErrorCode.IdentityConflictError)
      case e: CqlException if !identityHasConflict(e.getMessage, entity) => for {
        identities <- identitiesWithConflicts(entity, identities).catchAll(_ => ZIO.succeed(Map.empty))
        msg        = if(identities.nonEmpty) prepareMessage(identities) else e.getMessage
      } yield IdentityConflictError(msg, ErrorCode.Unknown)
    }
  } yield uuid

  protected def identityHasConflict(msg: String, entity: EntityRef): Boolean =
    Regex(s"Condition on table ${db.h.eidNskey(entity).name} was not satisfied")
      .findFirstMatchIn(msg) match {
      case Some(_) => true
      case None => false
    }

  protected def identitiesWithConflicts(entity: EntityRef, identities: Seq[Identity]): ZIO[Any, Throwable, Map[NamespaceRef, Seq[Identity]]] = for {
    identitiesAndUuids             <- eidsForIdentities(entity, identities)
    (oldIdentities, newIdentities) = divideOldAndNewIdentities(identitiesAndUuids)
    allIdentitiesWithSameUuid      <- allIdentitiesWithSameUUID(entity, groupByUuid(oldIdentities.map((identity, uuidOpt) => identity -> uuidOpt.get)))
    groupedIdentities              = grouped(allIdentitiesWithSameUuid, newIdentities)
  } yield groupedIdentities

  protected def grouped(allIdentitiesWithSameUuid:Map[UUID, Seq[(Identity, UUID)]], newIdentities: Seq[(Identity, Option[UUID])]): Map[NamespaceRef, Seq[Identity]] = {
    val flatOldIdentities = allIdentitiesWithSameUuid.toSeq.flatMap((_, seq) => seq.map(_._1))
    val allIdentities     = flatOldIdentities ++ newIdentities.map(_._1)
    allIdentities.groupBy(_.namespaceRef).filter(m => m._2.size > 1 && m._2.size == m._2.toSet.size)
  }

  protected def prepareMessage(grouped: Map[NamespaceRef, Seq[Identity]]): String = grouped.toSeq.map((ns, seqIdentities) => s"For namespace ${ns.name} there are conflicting identities with keys:" + seqIdentities.map(_.key).mkString(",") ).mkString(". ")

  protected def eidsForIdentities(entity: EntityRef, identities: Seq[Identity]): ZIO[Any, Throwable, Seq[(Identity, Option[UUID])]] = ZIO.foreachPar(identities)(identity =>
    for {
      _               <- isValidIdentityKey(identity.key) ensureOrFail (ClientError(s"Identity key ${identity.key} is not valid"))
      optionUUIDTuple <- db.selectOne[Tuple1[UUID]](cql"SELECT eid FROM ${db.h.nskeyEid(entity)} WHERE key = ${identity.key} AND ns_cid = ${identity.namespaceRef.cid}")
      optionUUID      = optionUUIDTuple map (_._1)
    } yield (identity, optionUUID)
  )

  protected def divideOldAndNewIdentities(identitiesAndUuids: Seq[(Identity, Option[UUID])]): (Seq[(Identity, Option[UUID])], Seq[(Identity, Option[UUID])]) = identitiesAndUuids.partition((_, optUuid) => optUuid.isDefined)

  protected def newIdentities(identityAndUUId: Seq[(Identity, Option[UUID])]): Seq[Identity] = identityAndUUId.filter(_._2.isEmpty).map(_._1)

  protected def groupByUuid(identityAndUUId: Seq[(Identity, UUID)]) : Map[UUID, Seq[(Identity, UUID)]] = identityAndUUId.groupBy(_._2).view.mapValues(_.distinct).toMap

  protected def allIdentitiesWithSameUUID(entityRef: EntityRef, identitiesAndUUId: Map[UUID, Seq[(Identity, UUID)]]): ZIO[Any, Throwable, Map[UUID, Seq[(Identity, UUID)]]] =
    ZIO.foreach(identitiesAndUUId)((uuid, _) => for {
      seqKeyNsCid <- db.selectAll[(String, CID)](cql"SELECT key, ns_cid FROM ${db.h.eidNskey(entityRef)} WHERE eid = $uuid")
      identities  <- ZIO.foreach(seqKeyNsCid)((key, nsCid) => for {
        namespaceOpt <- catalog.namespace(entityRef, nsCid)
      } yield if (namespaceOpt.isDefined) Option(Identity(namespaceOpt.get, key) -> uuid) else None)
    } yield uuid -> identities.flatten)


  protected def prepareStatements(entity: EntityRef, oldIdentities: Map[UUID, Seq[(Identity, UUID)]], newIdentities: Seq[(Identity, Option[UUID])], sleep: Int = 0): Task[(UUID, Seq[CqlStatement])] = {
    if (oldIdentities.isEmpty)
    {
      val uuid = UUID.randomUUID()
      Task(uuid -> prepareInsertsForNewIdentities(entity, newIdentities, uuid))
    }
    else if (oldIdentities.size == 1) {
      Task(oldIdentities.head._1, prepareInsertsForNewIdentities(entity, newIdentities, oldIdentities.head._1))
    }
    else
    {
      val mostCommonUUID = findMostCommonUUID(oldIdentities)
      val preparedInserts = prepareInsertsForNewIdentities(entity, newIdentities, mostCommonUUID)

      {
        for {
          mergedInsert <- prepareMergedInsertStatement(entity, oldIdentities, mostCommonUUID, sleep)
        } yield mostCommonUUID -> (mergedInsert.flatten ++ preparedInserts ++ prepareUpdatesForOldEntities(entity, oldIdentities, mostCommonUUID))
      }
    }
  }

  protected def prepareMergedInsertStatement(entityRef: EntityRef, groupedIdentities : Map[UUID, Seq[(Identity, UUID)]], toUuid: UUID, sleep: Int = 0): ZIO[Any, Throwable, Seq[Option[CqlStatement]]] = ZIO.foreach(merged(groupedIdentities, toUuid))(merged => for {
    exists <- db.selectOne[Tuple1[Boolean]](cql"SELECT true FROM ${db.h.mergedEids(entityRef)} WHERE from_eid = ${merged.from} AND to_eid = ${merged.to}")
    _      <- logger.info(exists.toString)
    _      <- logger.info("sleep" + sleep.toString)
    _      <- ZIO.sleep(sleep.seconds)
    insert =  if(exists.isEmpty) Some(db.buildInsertStatement(db.h.mergedEids(entityRef).name, Map("from_eid" -> merged.from , "to_eid" -> merged.to), ifNotExists = true, elseError = true)) else None
  } yield insert).provide(Has(clock))

  protected def merged(groupedByUUID : Map[UUID, Seq[(Identity, UUID)]], toUuid: UUID): Seq[Merged] = groupedByUUID.filter(_._1 != toUuid).map((uuid, _) => Merged(uuid, toUuid)).toSeq

  protected def prepareInsertsForNewIdentities(entity: EntityRef, newIdentities: Seq[(Identity, Option[UUID])], uuid: UUID): Seq[CqlStatement] = newIdentities.flatMap((identity, _) => Seq(
    db.buildInsertStatement(db.h.nskeyEid(entity).name, Map("ns_cid" -> identity.namespaceRef.cid, "eid" -> uuid, "key" -> identity.key), ifNotExists = true, elseError = true),
    db.buildInsertStatement(db.h.eidNskey(entity).name, Map("ns_cid" -> identity.namespaceRef.cid, "eid" -> uuid, "key" -> identity.key), ifNotExists = true, elseError = true),
  ))

  protected def prepareUpdatesForOldEntities(entity: EntityRef, oldIdentities: Map[UUID, Seq[(Identity, UUID)]], uuid: UUID): Seq[CqlStatement] =
    oldIdentities.filter(_._1 != uuid).toSeq.flatMap((oldEid, seqOfIdentities) =>
      seqOfIdentities.flatMap((identity, _) => Seq(
        db.buildUpdateStatement(db.h.nskeyEid(entity).name, Map("eid" -> uuid), cql"ns_cid = ${identity.namespaceRef.cid} AND key = ${identity.key}"),
        db.buildInsertStatement(db.h.eidNskey(entity).name, Map("eid" -> uuid, "ns_cid" -> identity.namespaceRef.cid, "key" -> identity.key), ifNotExists = true, elseError = true),
        cql"DElETE FROM ${db.h.eidNskey(entity)} WHERE eid = $oldEid;"
      )))

  protected def findMostCommonUUID(groupedIdentities : Map[UUID, Seq[(Identity, UUID)]]): UUID = groupedIdentities.map(uuidAndSeq => uuidAndSeq._1 -> uuidAndSeq._2.size).maxBy(_._2)._1
}
