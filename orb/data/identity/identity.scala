package orb.data.identity

import orb.core.*
import orb.sugar.*
import zio.*
import orb.cql.{CqlInvalidQueryException, decode, *}
import orb.api.http.data.{Identity, RequestIdentity}

import java.util.UUID
import orb.catalog.Catalog
import zio.clock.*
import zio.logging.Logging
import com.datastax.driver.core.exceptions.InvalidQueryException
import scala.util.matching.Regex

type IdentityRepo = Has[Service]

case class IdentityRef(entity: EntityRef, eid: UUID, identityNs: NamespaceRef, identityKey: Identifier, mergedEids: Seq[UUID])

class IdentityRepoError(msg: String, code: ErrorCode) extends ServiceError(msg, code)
case class ClientError(msg: String, code: ErrorCode = ErrorCode.Unknown) extends IdentityRepoError(msg, code) with ClientErrorTag
case class InternalError(msg: String, code: ErrorCode = ErrorCode.Unknown) extends IdentityRepoError(msg, code)
case class IdentityConflictError(msg: String, code: ErrorCode) extends IdentityRepoError(msg, code)

def IdentityRepoTask[R, E <: Throwable, A](zio: ZIO[R, E, A]): ZIO[R, IdentityRepoError, A] = zio.refineOrDie {
    case e: IdentityRepoError => e
    case c: (ServiceError & ClientErrorTag) => ClientError(c.getMessage, c.getCode)
    case s: ServiceError => InternalError(s.getMessage, s.getCode)
    case t: Throwable => InternalError(t.getMessage)
  }

type IdentityRepoTask[A] = IO[IdentityRepoError, A]
type IdentityClockRepoTask[A] = ZIO[Clock, IdentityRepoError, A]

trait Service {
  def ensureIdentity(entity: EntityRef, namespace: NamespaceRef, key: Identifier): IdentityRepoTask[IdentityRef]
  def findIdentities(entity: EntityRef, eid: UUID): IdentityRepoTask[Seq[IdentityRef]]
  def getOrCreateIdentity(entity: EntityRef, identities: Seq[Identity], sleep: Int = 0): IdentityRepoTask[UUID]
  def getIdsPartitioned(entity: EntityRef,
                        perPartitionLimit: Int,
                        scaleFactor:Int = 10,
                        parallelTasks:Int = 16): IdentityRepoTask[Seq[String]]
}

val dummy: ULayer[IdentityRepo] = ZLayer.succeed(new Service {
  override def ensureIdentity(entity: EntityRef, namespace: NamespaceRef, key: Identifier): IdentityRepoTask[IdentityRef] =
    Task.succeed(IdentityRef(entity, UUID.randomUUID(), namespace, key, Seq.empty))

  override def findIdentities(entity: EntityRef, eid: UUID): IdentityRepoTask[Seq[IdentityRef]] = Task.succeed(Seq.empty)

  override def getOrCreateIdentity(entity: EntityRef, identities: Seq[Identity], sleep: Int = 0): IdentityRepoTask[UUID] = Task.succeed(UUID.randomUUID())

  override def getIdsPartitioned(entity: EntityRef,
                                 perPartitionLimit: Int,
                                 scaleFactor:Int = 10,
                                 parallelTasks:Int = 16): IdentityRepoTask[Seq[String]] = Task.succeed(Seq.empty)
})

  val live: ZLayer[CQL & Catalog & Clock & Logging, Nothing, IdentityRepo] = {
    for {
      db      <- ZIO.access[CQL](_.get)
      catalog <- ZIO.access[Catalog](_.get)
      clock   <- ZIO.access[Clock](_.get)
      logging   <- ZIO.access[Logging](_.get)
    } yield new IdentityLive(db, catalog, clock, logging)
  }.toLayer