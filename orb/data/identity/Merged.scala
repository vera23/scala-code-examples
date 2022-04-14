package orb.data.identity

import orb.core.EntityRef
import orb.cql.Service as CqlService
import zio.*
import orb.cql.{given, _}
import orb.cql.decode.{given, _}
import orb.metrics.OrbMetricAspect
import zio.zmx.metrics.MetricsSyntax

import java.util.UUID

case class MergedEids(db: CqlService)
{
  def collectMergedEids(entityRef: EntityRef, currentEid: UUID): Task[Seq[UUID]] = for
      eidsOld <- {
        db.selectAll[Tuple1[UUID]](cql"SELECT from_eid FROM ${db.h.mergedEids(entityRef)} WHERE to_eid = $currentEid")
          @@ OrbMetricAspect("collectMergedEids_rps", Seq("action" -> "collectMergedEids_rps")).countRps
          @@ OrbMetricAspect("collectMergedEids_duration", Seq("action" -> "collectMergedEids_duration")).countDuration
      }
      eidsRec <- ZIO.foreachPar(eidsOld)(t => collectMergedEids(entityRef, t._1))
    yield eidsOld.map(_._1) ++ eidsRec.flatten
}

