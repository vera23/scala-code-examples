package orb.api.http.data

import zhttp.http._
import zio._
import zio.logging.Logging
import orb.catalog.{ Service => CatalogService, _ }
import orb.tree.{Service => TreeService, _}
import orb.data.writer.{ Service => WriterService, _ }
import orb.data.reader.{ Service => ReaderService, given, _ }
import orb.data.identity.{ Service => IdentityService, _ }
import orb.core._
import orb.api._
import orb.api.http.decode.given
import orb.http.ApiTask
import io.circe.generic.auto._
import java.util.UUID
import orb.compaction.{Service => CompactionService, *}
import orb.metrics.OrbMetricAspect
import zio.zmx.metrics.MetricsSyntax


enum ReadMode(val modeString: String) {
  case Merged extends ReadMode("merged")
  case Layered extends ReadMode("layered")
}

type Path = List[RequestPathSegment]
type ErrorMsq = String

case class FieldData(path: Path, storageLocation: Option[NodeStorageLocation], value: Any, parentIdsAndKeyValues: Map[Short, String], collectionColumnKeyAndValue: Map[String, String])
type WriteRequestParsingResult = Seq[Either[ErrorMsq, FieldData]]


case class RequestIdentity(namespace: String, key: String)

case class Identity(namespaceRef: NamespaceRef, key: String)

case class WriteRequestAttributes(parsingResults: WriteRequestParsingResult)

case class WriteRequestOptions(resilientWrite: Boolean)

case class IdentityResponse(namespace: Identifier, key: Identifier)
extension (id: IdentityRef) def toResponse: IdentityResponse = IdentityResponse(id.identityNs.name, id.identityKey)

case class LayeredReadResponse(identities: Option[Seq[IdentityResponse]], attrs: LayeredReadResultTree)
case class MergedReadResponse(identities: Option[Seq[IdentityResponse]], attrs: ReadResultTree)
case class WriteResponse(v: String)

def buildDataHttpApp: URIO[Catalog & Reader & Writer & IdentityRepo & Tree & Compaction, RHttpApp[Logging]] = for
  theReader   <- ZIO.service[ReaderService]
  theWriter   <- ZIO.service[WriterService]
  theCatalog  <- ZIO.service[CatalogService]
  theIdentity <- ZIO.service[IdentityService]
  theTree     <- ZIO.service[TreeService]
  compaction  <- ZIO.service[CompactionService]
yield httpApp(theReader, theWriter, theCatalog, theIdentity, theTree, compaction)

def httpApp(theReader: ReaderService, theWriter: WriterService, theCatalog: CatalogService, theIdentity: IdentityService, theTree: TreeService, theCompaction: CompactionService): RHttpApp[Logging] = {
  val dataConnector = Connector(theReader, theWriter, theCatalog, theTree, theIdentity)

  Http.collectM[Request] {
    case request@Method.GET -> Root / "entity" / entity  => ApiTask {
      for
        readRequest <- {
          dataConnector.buildReadRequest(entity, request)
            @@ OrbMetricAspect("r1_buildReadRequest_rps", Seq("action" -> "r1_buildReadRequest_rps")).countRps
            @@ OrbMetricAspect("r1_buildReadRequest_duration", Seq("action" -> "r1_buildReadRequest_duration")).countDuration
        }
        response    <- readRequest match {
          case l: LayeredReadRequest => for
            result   <- {
              dataConnector.readLayered(l)
                @@ OrbMetricAspect("r1_readLayered_rps", Seq("action" -> "r1_readLayered_rps")).countRps
                @@ OrbMetricAspect("r1_readLayered_duration", Seq("action" -> "r1_readLayered_duration")).countDuration
            }
            response =  ApiResponse.succeedZ(result)
          yield response
          case m: MergedReadRequest  => for
            result   <- {
              dataConnector.readMerged(m)
                @@ OrbMetricAspect("r1_readMerged_rps", Seq("action" -> "r1_readMerged_rps")).countRps
                @@ OrbMetricAspect("r1_readMerged_duration", Seq("action" -> "r1_readMerged_duration")).countDuration
            }
            response =  ApiResponse.succeedZ(result)
          yield response
        }
      yield response
    } @@ OrbMetricAspect("r0_read_rps", Seq("action" -> "r0_read_rps")).countRps

    case request@Method.POST -> Root / "entity" / entity => ApiTask {
      for {
        result <- dataConnector.write(entity, request)
        data = Map("uuid" -> result.uuid)
        errors = result.errors.map(e => ResponseError(ErrorCode.Unknown.n, e))
        response = if result.status == WriteStatus.Success
        then ApiResponse.succeedZ(data, errors)
        else ApiResponse.failedZ(data, errors)
      } yield response
    }

    case Method.GET -> Root / "compact" / entity / eid => for {
      entityRef <- theCatalog.ensureEntity(entity)
      uuid = UUID.fromString(eid)
      _         <- theCompaction.compact(uuid, entityRef)
    } yield Response.text(s"Compaction for '$entity' eid $uuid started.")

  }
}
