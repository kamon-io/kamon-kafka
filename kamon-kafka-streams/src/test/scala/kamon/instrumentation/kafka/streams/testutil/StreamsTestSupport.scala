package kamon.instrumentation.kafka.streams.testutil

import java.util.Properties

trait StreamsTestSupport {

  def map2Properties(map: Map[String, AnyRef]): Properties = {
    import scala.collection.JavaConverters._

    val props = new Properties
    props.putAll(map.asJava)
    props
  }
}
