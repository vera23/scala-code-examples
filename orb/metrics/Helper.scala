package orb.metrics

import orb.metrics.Counter.putRefIfAbsent
import zio.zmx.*
import zio.zmx.metrics.*
import zio.*
import zio.clock.*
import zio.duration.*

def aspCountAll(tags: Seq[Label] = Seq.empty): MetricAspect[Any] = MetricAspect.count("countAll", (tags :+ ("host", sys.env.getOrElse("HOSTNAME", "None"))) *)


def requestPerSecondGauge(tags: Seq[Label] = Seq.empty) = MetricAspect.setGauge("requestsPerSecond", (tags :+ ("host", sys.env.getOrElse("HOSTNAME", "None"))) *)


class OrbMetricAspect(name: String, tags: Seq[Label] = Seq.empty) {
  
  val countRps:MetricAspect[Any] = new MetricAspect {
      def apply[R, E, A1](zio: ZIO[R, E, A1]) = zio.tap(_ => orb.metrics.Counter.update(name, tags, 1.0))
  }

  val countDuration:MetricAspect[Any] = new MetricAspect{
    def apply[R, E, A1](zio: ZIO[R, E, A1]): ZIO[R, E, A1] =
      zio.timedWith(ZIO.succeed(System.nanoTime)).flatMap { case (duration, a) =>
        orb.metrics.Counter.update(name, tags, duration.toMillis.toDouble).as(a)
      }
  }
}