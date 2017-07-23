package gr.ml.analytics.batch.cf

import java.time.{Duration, Instant}

object TestUtil {

  def timed(metered: () => Unit): Long = {
    val start = Instant.now()

    metered.apply()

    val end = Instant.now()

    println(s"Start time $start")
    println(s"End time $end")

    val duration = Duration.between(start, end)
    duration.getSeconds
  }

  def printElapsedSec(sec: Long) = println(s"Elapsed time ${String.format("%s:%s:%s",
    (sec / 3600).toString,
    ((sec % 3600) / 60).toString,
    (sec % 60).toString)}")


}
