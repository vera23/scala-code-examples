package orb.compaction

import zio.*
import orb.core.*
import orb.cql.*
import orb.conf.*
import logging.Logging
import orb.tree.Tree
import java.util.UUID

type Compaction = Has[Service]

type CompactionFiber = Fiber[CompactionError, Unit]
class CompactionError(msg: String, code: ErrorCode = ErrorCode.Unknown) extends ServiceError(msg, code)
case class InternalError(msg: String, code: ErrorCode = ErrorCode.Unknown) extends CompactionError(msg, code)

type CompactionTask[A] = IO[CompactionError,A]


trait Service {
  def compact(eid: UUID, entity: EntityRef): CompactionTask[Unit]
}


val liveFromConfig: ZLayer[Conf & CQL & Logging, Throwable, Compaction] = (for {
      config   <- orb.conf.root
      logging <- ZIO.access[Logging](_.get)
      db       <- ZIO.access[CQL](_.get)
  } yield new CompactionLive(config.compaction.threshold, db, logging)).toLayer


def live(threshold: Int): ZLayer[CQL & Logging, Throwable, Compaction] = (for {
  logging <- ZIO.access[Logging](_.get)
  db      <- ZIO.access[CQL](_.get)
} yield new CompactionLive(threshold, db, logging)).toLayer
