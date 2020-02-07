/* =========================================================================================
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

val kamonCore           = "io.kamon"            %% "kamon-core"                       % "2.0.4"
val kamonTestkit        = "io.kamon"            %% "kamon-testkit"                    % "2.0.4"
val instrumentationCommon = "io.kamon"          %% "kamon-instrumentation-common"     % "2.0.1"
val kanela              = "io.kamon"             % "kanela-agent"                     % "1.0.5"

val kafkaClient         = "org.apache.kafka"    % "kafka-clients"	                    % "2.3.0"
val kafkaStreams        = "org.apache.kafka"    % "kafka-streams"	                    % "2.3.0"
val kafkaStreamsTestUtils    = "org.apache.kafka"    % "kafka-streams-test-utils"     % "2.3.0"
val kafkaStreamsScala   = "org.apache.kafka"    %% "kafka-streams-scala"	            % "2.3.0"

val kafkaTest           = "io.github.embeddedkafka" %% "embedded-kafka"               % "2.3.0"
val kafkaStreamTest     = "io.github.embeddedkafka" %% "embedded-kafka-streams"       % "2.3.0"


lazy val root = (project in file("."))
  .settings(noPublishing: _*)
  .aggregate(kafkaClients, kafkaStream)

lazy val kafkaClients = (project in file("kamon-kafka-clients"))
  .settings(instrumentationSettings)
  .enablePlugins(JavaAgent)
  .settings(bintrayPackage := "kamon-kafka")
  .settings(name := "kamon-kafka")
  .settings(scalaVersion := "2.12.9")
  .settings(crossScalaVersions := Seq("2.11.12", "2.12.9"))
  .settings(
    libraryDependencies ++=
      compileScope(kamonCore, kafkaClient, instrumentationCommon) ++
      providedScope(kanela) ++
      testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic, kafkaTest))

lazy val kafkaStream = (project in file("kamon-kafka-streams"))
  .settings(instrumentationSettings)
  .enablePlugins(JavaAgent)
  .dependsOn(kafkaClients % "compile->compile;test->test")
  .settings(bintrayPackage := "kamon-kafka")
  .settings(name := "kamon-kafka-streams")
  .settings(scalaVersion := "2.12.9")
  .settings(crossScalaVersions := Seq("2.11.12", "2.12.9"))
  .settings(
    libraryDependencies ++=
      compileScope(kamonCore, kafkaStreams, kafkaStreamsScala, instrumentationCommon) ++
      providedScope(kanela) ++
      testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic, kafkaStreamTest, kafkaStreamsTestUtils))
