/* =========================================================================================
 * Copyright © 2013-2019 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */
package kamon.kafka.testutil

import kamon.testkit.TestSpanReporter
import kamon.trace.Span
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}

abstract class SpanReportingTestScope(_reporter: TestSpanReporter.BufferingSpanReporter) extends Eventually with Matchers {
  private var _reportedSpans: List[Span.Finished] = Nil
  _reporter.clear()

  def reportedSpans: List[Span.Finished] = _reportedSpans

  def assertNoSpansReported(): Unit =
    _reporter.nextSpan() shouldBe None

  def awaitNumReportedSpans(numOfExpectedSpans: Int)(implicit timeout: PatienceConfiguration.Timeout): Unit = {
    eventually(timeout) {
      collectReportedSpans()
//      println(s"_reportedSpans.size=${_reportedSpans.size}")
      _reportedSpans should have size numOfExpectedSpans
    }
    Thread.sleep(1000)
    if(_reporter.nextSpan().isDefined) {
      fail(s"Expected only $numOfExpectedSpans spans to be reported, but got more!")
    }
  }

  def collectReportedSpans(): Unit =
    _reporter.nextSpan().foreach { s =>
      _reportedSpans = s :: _reportedSpans
    }

  def assertReportedSpan[T](p: Span.Finished => Boolean)(f: Span.Finished => T): T = {
    _reportedSpans.filter(p) match {
      case Nil =>
        fail("expected span not found!")
      case x :: Nil =>
        f(x)
      case x :: y =>
        fail(s"More than one span found! spans:${x :: y}")
    }
  }

  def assertReportedSpans(p: Span.Finished => Boolean)(f: List[Span.Finished] => Unit): Unit = {
    _reportedSpans.filter(p) match {
      case Nil =>
        fail("expected span not found!")
      case x =>
        f(x)
    }
  }
}
