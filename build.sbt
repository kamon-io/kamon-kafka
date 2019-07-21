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

val kamonCore           = "io.kamon"            %% "kamon-core"                       % "2.0.0-RC1"
val kamonTestkit        = "io.kamon"            %% "kamon-testkit"                    % "2.0.0-RC1"
val scalaExtension      = "io.kamon"            %% "kamon-instrumentation-common"     % "2.0.0-RC2"

val kafkaClient         = "org.apache.kafka"    % "kafka-clients"	                    % "0.11.0.0"
val kafkaStreams        = "org.apache.kafka"    % "kafka-streams"	                    % "2.0.0"

val kafkaTest           = "net.manub"           %% "scalatest-embedded-kafka"         % "2.0.0"
val kafkaStreamTest     = "net.manub"           %% "scalatest-embedded-kafka-streams" % "2.0.0"

val lombok              = "org.projectlombok"   % "lombok"                            % "1.18.0"


lazy val root = (project in file("."))
  .settings(noPublishing: _*)
  .aggregate(kafkaClients, kafkaStream)

lazy val kafkaClients = (project in file("kamon-kafka-clients"))
  .enablePlugins(JavaAgent)
  .settings(bintrayPackage := "kamon-kafka")
  .settings(name := "kamon-kafka-clients")
  .settings(scalaVersion := "2.12.8")
  .settings(crossScalaVersions := Seq("2.11.12", "2.12.8"))
  .settings(javaAgents += "io.kamon" % "kanela-agent" % "1.0.0-RC4" % "compile;test")
  .settings(resolvers += Resolver.bintrayRepo("kamon-io", "snapshots"))
  .settings(resolvers += Resolver.mavenLocal)
  .settings(
      libraryDependencies ++=
        compileScope(kamonCore, kafkaClient, scalaExtension) ++
        providedScope(lombok) ++
        testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic, kafkaTest))

lazy val kafkaStream = (project in file("kamon-kafka-streams"))
  .enablePlugins(JavaAgent)
  .dependsOn(kafkaClients % "compile->compile;test->test")
  .settings(bintrayPackage := "kamon-kafka")
  .settings(name := "kamon-kafka-streams")
  .settings(scalaVersion := "2.12.8")
  .settings(crossScalaVersions := Seq("2.11.12", "2.12.8"))
  .settings(javaAgents += "io.kamon"    % "kanela-agent"   % "1.0.0-RC4"  % "compile;test")
  .settings(resolvers += Resolver.bintrayRepo("kamon-io", "snapshots"))
  .settings(resolvers += Resolver.mavenLocal)
  .settings(
        libraryDependencies ++=
          compileScope(kamonCore, kafkaStreams, scalaExtension) ++
            providedScope(lombok) ++
            testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic, kafkaStreamTest))
