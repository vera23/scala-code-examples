package orb.data.writer

import io.circe
import io.circe.Json
import zio.*
import orb.api.http.data.*
import orb.core.{EnumRef, LayerRef}
import orb.catalog.*
import orb.tree.*
import orb.cql.*
import orb.data.writer.LiveWriter
import zio.logging.Logging
import orb.data.identity.*
import zio.clock.*
import orb.compaction.{Compaction, CompactionFiber}
import io.circe.DecodingFailure
import zmx.metrics.*
import orb.metrics.*
import zio.duration.*

import java.util.UUID

type Writer = Has[Service]

enum WriteStatus {
  case Success
  case Error
}

case class WriteResult(uuid: Option[UUID], status: WriteStatus, errors: Seq[String])


trait Service {
  def parseFromWriteToWriteRequest(tree: RootNode, enums: Seq[EnumRef], write: Write): Either[io.circe.Error, WriteRequest]
  def write(layerRef: LayerRef, writeRequest: WriteRequest): Task[(WriteResult, CompactionFiber)]
}


val live: ZLayer[Catalog & Tree & CQL & IdentityRepo & logging.Logging & Clock & Compaction, Throwable, Writer] = (for {
  catalog    <- ZIO.access[Catalog](_.get)
  db         <- ZIO.access[CQL](_.get)
  tree       <- ZIO.access[Tree](_.get)
  identity   <- ZIO.access[IdentityRepo](_.get)
  logging    <- ZIO.access[Logging](_.get)
  clock      <- ZIO.access[Clock](_.get)
  compaction <- ZIO.access[Compaction](_.get)
  //_          <- counterMetrics("write", tags = Seq("action" -> "write")).repeat(Schedule.fixed(20.second)).fork
} yield new LiveWriter(catalog, tree, db, identity, logging, clock, compaction)).toLayer


case class WriteRequest(
                         layer: String,
                         identities: List[RequestIdentity],
                         options: WriteRequestOptions,
                         attributes: WriteRequestAttributes
                       )


case class Options(resilientWrite:Boolean)
case class Write(options:Options, layer:String, identities:Seq[RequestIdentity], attributes:Json)


