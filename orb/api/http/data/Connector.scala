package orb.api.http.data

import zhttp.http._
import zio._
import orb.data.reader.{ Service => ReaderService, _ }
import orb.data.writer.{ Service => WriterService, _ }
import orb.data.identity.{ Service => IdentityService, _ }
import orb.catalog.{ Service => CatalogService, _ }
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import orb.sugar._
import orb.api.http.decode.QueryParamsDecoder
import orb.catalog.Catalog
import orb.tree._

import orb.api.http.decode.{given, _}
import orb.core._
import cats.implicits._

import java.util.UUID
import java.math.BigDecimal

import orb.api.http.given
import orb.data.writer.given

case class  Connector(reader: ReaderService, writer: WriterService, theCatalog: orb.catalog.Service, theTree: orb.tree.Service, identityService: IdentityService) {
  /**
   * Read request QueryParams mapping
   */
  case class ReadQuery(
    mode: ReadMode,
    identityNs: String,
    identityKey: String,
    paths: List[String],
    withIdentities: Boolean,
    withTimestamps: Boolean,
    onlyLayers: List[String],
    excludeLayers: List[String],
    mergePolicy: MergePolicy,
    layersPriority: Seq[String],
  ) {
  }
  object ReadQuery {
    val defaults: QueryParams = Map(
      "mode" -> List(ReadMode.Layered.modeString),
      "paths" -> List.empty[String],
      "withIdentities" -> List("false"),
      "withTimestamps" -> List("false"),
      "onlyLayers" -> List.empty[String],
      "excludeLayers" -> List.empty[String],
      "mergePolicy" -> List(MergePolicy.Latest.policyString),
      "layersPriority" -> List.empty[String],
    )
  }

  def readLayered(request: LayeredReadRequest): ReadTask[LayeredReadResponse] = for
    readResult <- reader.readLayered(request)
    resultTree <- ReadResultTree.fromLayeredResultSet(readResult.attributes)
  yield LayeredReadResponse(readResult.identities map (_ map (_.toResponse)), resultTree)

  def readMerged(request: MergedReadRequest): ReadTask[MergedReadResponse] = for
    readResult <- reader.readMerged(request)
    resultTree <- ReadResultTree.fromResultSet(readResult.attributes)
  yield MergedReadResponse(readResult.identities map (_ map (_.toResponse)), resultTree)

  def write(entity: String, request: Request): Task[WriteResult] = for {
    entityRef                     <- theCatalog.ensureEntity(entity)
    rootNode                      <- theTree.getAttributeTree(entityRef)
    enums                         <- theCatalog.listEnums(entityRef)
    writeRequest                  <- buildWriteRequest(request, rootNode, enums)
    layerRef                      <- theCatalog.ensureLayer(entityRef, writeRequest.layer)
    (writeResult, compactionFib)  <- writer.write(layerRef, writeRequest)
  } yield writeResult

  def buildReadRequest(entityName: String, request: zhttp.http.Request)(using decoder: QueryParamsDecoder[ReadQuery]): ReadTask[ReadRequest] = ReadTask {
    for {
      entity         <- theCatalog.ensureEntity(entityName)
      queryParams    = request.url.queryParams
      readQuery      <- ZIO.fromEither(decoder.decodeParams(queryParams, ReadQuery.defaults))
      identityNs     <- theCatalog.ensureNamespace(entity, readQuery.identityNs)
      identity       <- identityService.ensureIdentity(entity, identityNs, readQuery.identityKey.asIdentifier)
      paths          <- buildPaths(entity, readQuery.paths)
      onlyLayers     <- buildLayers(entity, readQuery.onlyLayers.distinct).map(_.toSet)
      excludeLayers  <- buildLayers(entity, readQuery.excludeLayers.distinct).map(_.toSet)
      layersPriority <- buildLayers(entity, readQuery.layersPriority).map(_.toVector)
    } yield readQuery.mode match {
      case ReadMode.Layered => LayeredReadRequest(
        entity,
        identity.eid,
        identity.mergedEids,
        paths,
        readQuery.withTimestamps,
        readQuery.withIdentities,
        onlyLayers,
        excludeLayers,
      )
      case ReadMode.Merged => MergedReadRequest(
        entity,
        identity.eid,
        identity.mergedEids,
        paths,
        readQuery.withTimestamps,
        readQuery.withIdentities,
        onlyLayers,
        excludeLayers,
        readQuery.mergePolicy,
        layersPriority
      )
    }
  }

  def buildPaths(entity: EntityRef, ls: List[String]): Task[List[RequestPath]] = for
    tree       <- theTree.getAttributeTree(entity)
    segments   =  ls map (_.split("\\.", -1))
    pathResult =  segments.map(orb.data.RequestPath.build(tree, _)).sequence
    path       <- pathResult getOrFail (ReadRequestError(_))
  yield path

  def buildLayers(entity: EntityRef, ls: Seq[String]): Task[Seq[LayerRef]] = {
    val mapped = ls.map{ layer => theCatalog.ensureLayer(entity, layer) }
    Task.collectAll(mapped)
  }
  
  private def buildWriteRequest(request: Request, rootNode: RootNode, enumSeq: Seq[EnumRef]): Task[WriteRequest] = {
    given node:RootNode      = rootNode
    given enums:Seq[EnumRef] = enumSeq

    for {
      body <- ZIO.fromOption(request.getBodyAsString).mapError(_ => Throwable("Request body is empty"))
      writeReq <- ZIO.fromEither(decode[WriteRequest](body)).mapError(e => Throwable(e))
    } yield writeReq
  }
}
