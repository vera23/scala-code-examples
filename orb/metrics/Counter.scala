package orb.metrics

import zio.*

import scala.collection.JavaConverters.*
import zio.clock.*
import zio.zmx.*
import zio.zmx.metrics.*
import zio.duration.*

import java.util.concurrent.ConcurrentHashMap

case class MetricsData(counter: Ref[Double], tags: Seq[Label] = Seq.empty)

object State {
  val map = new ConcurrentHashMap[String, MetricsData]
}

object Counter {

  def get(name: String):MetricsData = State.map.get(name)

  def getOrCreate(name: String, tags: Seq[Label] = Seq.empty): UIO[MetricsData] = for {
    _     <- putRefIfAbsent(name, tags)
    value <- UIO(State.map.get(name))
  } yield value

  def update(name: String, tags: Seq[Label], update: Double = 1.0) = for {
    _ <- putRefIfAbsent(name, tags)
    _ <- State.map.get(name).counter.update(_ + update)
  } yield ()

  def set(name: String, tags: Seq[Label], newValue: Double) = for {
    _     <- putRefIfAbsent(name, tags)
    _ <- State.map.get(name).counter.update(_  => newValue)
  } yield ()

  protected def putRefIfAbsent(name: String, tags: Seq[Label]) = for {
    ref <- zio.Ref.make(0.0)
    _   <- UIO(State.map.putIfAbsent(name, MetricsData(ref, tags)))
  } yield ()

}

type Metrics = Has[Service]


trait Service {
  def executeDaemon(): zio.ZIO[Any, Nothing, Unit]
}


object MetricsCounter extends Service {

  override def executeDaemon(): zio.ZIO[Any, Nothing, Unit]= for {
    st <- ZIO.succeed(State.map.keys().asIterator().asScala.toSeq)
    _ <- ZIO.foreachPar(st) { (name) => for {
        metricData <- ZIO.succeed(orb.metrics.Counter.get(name))
        _          <- metricData.counter.get @@ requestPerSecondGauge(metricData.tags)
        _          <- orb.metrics.Counter.set(name, metricData.tags, 0.0)
      } yield ()
    }
  } yield ()
}

val live = {
  for {
    _ <- MetricsCounter.executeDaemon().repeat(Schedule.spaced(1.second)).forkDaemon
  } yield ()
}.toLayer
