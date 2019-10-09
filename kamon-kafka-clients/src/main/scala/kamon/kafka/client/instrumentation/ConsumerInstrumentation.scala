/*
 * =========================================================================================
 * Copyright © 2013-2019 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
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

package kamon.kafka.client.instrumentation

import java.time.Duration

import kamon.kafka.client.instrumentation.advisor.PollMethodAdvisor
import kanela.agent.api.instrumentation.InstrumentationBuilder

class ConsumerInstrumentation extends InstrumentationBuilder {

  /**
    * Instruments org.apache.kafka.clients.consumer.KafkaConsumer::poll(Long)
    * Kafka version < 2.3
    */
  onType("org.apache.kafka.clients.consumer.KafkaConsumer")
    .advise(method("poll").and(withArgument(0, classOf[Long])), classOf[PollMethodAdvisor])

  /**
    * Instruments org.apache.kafka.clients.consumer.KafkaConsumer::poll(Duration)
    * Kafka version >= 2.3
    */
  onType("org.apache.kafka.clients.consumer.KafkaConsumer")
    .advise(method("poll").and(withArgument(0, classOf[Duration])), classOf[PollMethodAdvisor])
}

