package kamon.kafka

import java.io.ByteArrayOutputStream

import com.typesafe.config.ConfigFactory
import kamon.context.BinaryPropagation.{ByteStreamReader, ByteStreamWriter}
import kamon.context.{BinaryPropagation, Context}
import kamon.trace.Span

object ContextSpanBinaryEncoder {

  private lazy val binaryPropagation = BinaryPropagation.from(ConfigFactory.load().getConfig("kamon.propagation.binary.default"))

  def decode(bytes: Array[Byte]): Context = {
    binaryPropagation.read(ByteStreamReader.of(bytes))
  }

  def encode(context: Context, span: Span): Array[Byte] = {
    val ctx = context.withKey(Span.Key, span)
    val baos = new ByteArrayOutputStream(1024) with ByteStreamWriter
    binaryPropagation.write(ctx, baos)
    baos.toByteArray
  }
}
