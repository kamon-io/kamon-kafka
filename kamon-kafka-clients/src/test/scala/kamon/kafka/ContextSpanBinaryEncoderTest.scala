package kamon.kafka

import kamon.Kamon
import org.scalatest.{Matchers, WordSpec}

class ContextSpanBinaryEncoderTest extends WordSpec with Matchers {

  "A ContextSpanBinaryEncoder" must {
    "encode from and decode to original value" in {

      val ctx = Kamon.currentContext()
      val span = Kamon.currentSpan()

      val encodedBytes = ContextSpanBinaryEncoder.encode(ctx, span)

      ContextSpanBinaryEncoder.decode(encodedBytes) shouldBe ctx
    }
  }

}
