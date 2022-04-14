package orb.http

import zio.*
import zhttp.http.*
import zhttp.service.Server
import zio.logging.*
import orb.*
import orb.orblet.*
import orb.catalog.Catalog
import orb.tree.Tree
import orb.nextid.NextId
import orb.data.reader.Reader
import orb.data.writer.Writer
import orb.data.identity.IdentityRepo
import orb.api.http.catalog.buildCatalogHttpApp
import orb.api.http.tree.buildTreeHttpApp
import orb.api.http.data.buildDataHttpApp
import orb.api.http.dashboard.buildDashboardHttpApp
import orb.cql.{given, *}
import orb.cql.decode.{given, *}
import orb.api.http.decode.*
import orb.api.{Response as _, *}
import orb.core.{ClientErrorTag, ServiceError}
import io.circe.{Encoder, Json}
import orb.sugar.*
import orb.compaction.*
import zhttp.http.Response.HttpResponse
import zio.Cause.{Die, Fail}
import zio.zmx.prometheus.PrometheusClient
import zio.zmx.MetricSnapshot.*

def globalHttpAppHandler(httpApp:RHttpApp[Logging & Has[PrometheusClient]]):RHttpApp[Logging & Has[PrometheusClient]] = httpApp.map(globalResponseHandler)

def globalResponseHandler(r:Response[Logging & Has[PrometheusClient],Throwable]):Response[Logging & Has[PrometheusClient],Throwable] = r match {
  case httpResponse: HttpResponse[_,_] => globalHttpResponseHandler(httpResponse.asInstanceOf[HttpResponse[Logging,Throwable]])
  case _ => r
}

extension [R,E](response:HttpResponse[R,E]) def withHeader(header:String, value:String):HttpResponse[R,E] = response.copy(headers = response.headers :+ Header.custom(header, value))

def globalHttpResponseHandler(r:HttpResponse[Logging,Throwable]):HttpResponse[Logging,Throwable] = {
  r.withHeader("X-Orb-Hostname",  sys.env.getOrElse("HOSTNAME", "None"))
}

def buildHttpApp: URIO[Has[PrometheusClient] & Catalog & Reader & Writer & IdentityRepo & Orblet & Tree & CQL & NextId & Logging & Compaction, RHttpApp[Logging & Has[PrometheusClient]]] = for {
  mainApp      <- buildMainHttpApp
  catalogApp   <- buildCatalogHttpApp
  dataApp      <- buildDataHttpApp
  treeApp      <- buildTreeHttpApp
  dashboardApp <- buildDashboardHttpApp
  app = mainApp +++ catalogApp +++ dataApp +++ treeApp +++ dashboardApp
} yield globalHttpAppHandler(app)

def buildMainHttpApp: URIO[Has[PrometheusClient] & Catalog & Orblet & CQL & NextId, RHttpApp[Logging & Has[PrometheusClient]]] = for {
  catalog <- ZIO.access[Catalog](_.get)
  orblet  <- ZIO.access[Orblet](_.get)
  cql     <- ZIO.access[CQL](_.get)
  nextid  <- ZIO.access[NextId](_.get)
} yield httpApp(catalog, orblet, cql, nextid)

def ApiTask[R <: Logging,E,A <: Response[_,_]](zio: ZIO[R,E,A]): ZIO[R,HttpError,A] = {
  zio.sandbox
    .tapError { cause => cause.prettyPrint.logError when cause.died || cause.interrupted || isInternalFailure(cause) }
    .mapError {
      cause => {
        val z = ApiResponse.failedUnknown.toZhttpError
        cause.foldLeft(z) {
          case (_, Fail(e)) => handleError(e)
          case (_, Die(t)) => handleError(t) // todo hide error later
        }
      }
    }
}

def ApiTaskSucceed[R <: Logging, E, A : Encoder](zio: ZIO[R,E,A]): ZIO[R,HttpError,UResponse] =
  ApiTask { for result <- zio yield ApiResponse.succeedZ(result) }
def ApiTaskSucceed[R <: Logging, E](zio: ZIO[R,E,Unit]): ZIO[R,HttpError,UResponse] =
  ApiTask { for _ <- zio yield ApiResponse.succeedZ }

private def isInternalFailure(cause: => Cause[Any]): Boolean = cause.failureOption match {
  case Some(er) => er match {
    case _: ClientErrorTag => false
    case _ => true
  }
  case _ => false
}

private def handleError(e: Any): HttpError = e match {
  case t: (ServiceError & ClientErrorTag) => HttpError.BadRequest(ApiResponse.failedS(t).toJsonString)
  case t: ServiceError                    => ApiResponse.failedS(t).toZhttpError
  case t: Throwable                       => ApiResponse.failedS(ServiceError.unknown(t.getMessage)).toZhttpError
  case _                                  => ApiResponse.failedUnknown.toZhttpError
}

// every compoent might expose their API, and we just compose them with ++
protected def httpApp(
  theCatalog: catalog.Service,
  theOrblet: orblet.Service,
  db : orb.cql.Service,
  nextId: orb.nextid.Service
  ): RHttpApp[Logging & Has[PrometheusClient]] = {

  // TODO: вынести всё в debug panel/dashboard
  def countTables(tables:Seq[String]):CqlTask[Map[String,Long]] = for {
    pairs <- ZIO.foreach(tables)(t => db.countPartitioned(t).map(t -> _))
  } yield pairs.toMap

  val orblet = Http.collectM[Request] {
    case request @ Method.GET -> Root / "cql" => for {
      q   <- request.url.queryParams.get("q").map(_.head) getOrFail (new Exception("q is not present"))
      res <- db.selectAll[Json](CqlStatement(q, Seq())) catchAll{ case e => IO.fail(new Exception(e.toString)) }
    } yield Response.text(Map("Query" -> q, "Result" -> res.toString).toString)
    case Method.GET -> Root / "health" => for uuid <- theOrblet.uuid yield Response.text(uuid.toString)
    case Method.GET -> Root / "ping" => log.info("ponged") *> ZIO.effectTotal(Response.text("pong"))
    case Method.GET -> Root / "orblets" => for rows <- db.selectAll[CqlRecord](cql"SELECT id, status, details FROM orblets") yield Response.text(rows.toString)
    case Method.GET -> Root / "nextid" / scope => for nid <- nextId.nextId(scope) yield Response.text(s"Next id for scope '$scope' is $nid.")

    case Method.GET -> Root / "count" / "table" / "all" => ApiTask { for
      tables <- db.selectAll[Tuple1[String]](cql"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ${db.h.defaultKeyspace}")
      map <- countTables(tables.map(_._1))
      yield Response.text(map.toString)
    }

    case Method.GET -> Root / "countcompare" / "table" / "all" => ApiTask { for
      tables <- db.selectAll[Tuple1[String]](cql"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ${db.h.defaultKeyspace}")
      map <- countTables(tables.map(_._1))
      map2 <- ZIO.foreach(tables)(t => db.selectCell[Long](cql"SELECT COUNT(*) FROM ${CqlExpr(db.h.defaultKeyspace)}.${CqlExpr(t._1)}").map(t._1 -> _.getOrElse(0)))
      zipped = (map.toList ++ map2.toList).groupBy(_._1).values.map(_.map(_._2))
      yield Response.text(zipped.toString)
    }

    case Method.GET -> Root / "count" / "table" / table => ApiTask { for
      map <- countTables(Seq(table))
    yield Response.text(map.toString)
    }

    case Method.GET -> Root / "metrics" => PrometheusClient.snapshot.map { case Prometheus(value) => Response.text(value) }

  }

  orblet
}
