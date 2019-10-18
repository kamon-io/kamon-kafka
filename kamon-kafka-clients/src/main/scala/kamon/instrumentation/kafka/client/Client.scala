package kamon.instrumentation.kafka.client

import com.typesafe.config.Config
import kamon.Kamon

object Client {

  val basePath = "kamon.instrumentation.kafka.client"

  @volatile var followStrategy: Boolean = followStrategyFromConfig(Kamon.config())
  @volatile var useDelayedSpans: Boolean = useDelayedSpansFromConfig(Kamon.config())

  private def followStrategyFromConfig(config: Config): Boolean =
    Kamon.config.getBoolean(basePath + ".follow-strategy")

  private def useDelayedSpansFromConfig(config: Config): Boolean =
    Kamon.config.getBoolean(basePath + ".use-delayed-spans")

  Kamon.onReconfigure( (newConfig: Config) => {
      followStrategy = followStrategyFromConfig(newConfig)
      useDelayedSpans = useDelayedSpansFromConfig(newConfig)
    }
  )
}
