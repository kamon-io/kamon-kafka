/* =========================================================================================
 * Copyright © 2013-2018 the kamon project <http://kamon.io/>
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


val kamonCore           = "io.kamon"            %% "kamon-core"               % "1.1.3"
val kamonTestkit        = "io.kamon"            %% "kamon-testkit"            % "1.1.3"
val scalaExtension      = "io.kamon"            %% "kanela-scala-extension"   % "0.0.10"

val kafkaClient         = "org.apache.kafka"    % "kafka-clients"	            % "0.11.0.0"

val kafkaTest = "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"

//val kafkaSpring        = "org.springframework.kafka"      % "spring-kafka"        	    % "1.3.3.RELEASE"
//val kafkaSpringTest    = "org.springframework.kafka"      % "spring-kafka-test"   	    % "1.3.3.RELEASE"
//val assert4j    = "org.assertj" % "assertj-core" % "3.11.1"
val lombok             = "org.projectlombok"         % "lombok"                    % "1.18.0"   


lazy val root = (project in file("."))
  .settings(noPublishing: _*)
  .aggregate(kafkaClients)

lazy val kafkaClients = (project in file("kamon-kafka-clients"))
  .enablePlugins(JavaAgent)
  .settings(bintrayPackage := "kamon-kafka")
  .settings(name := "kamon-kafka-clients")
  .settings(scalaVersion := "2.12.8")
  .settings(crossScalaVersions := Seq("2.11.12", "2.12.8"))
  .settings(javaAgents += "io.kamon"    % "kanela-agent"   % "0.0.15"  % "compile;test")
  .settings(resolvers += Resolver.bintrayRepo("kamon-io", "snapshots"))
  .settings(resolvers += Resolver.mavenLocal)
  .settings(
      libraryDependencies ++=
        compileScope(kamonCore, kafkaClient, scalaExtension) ++
        providedScope(lombok) ++
        testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic, kafkaTest))

//lazy val kafkaStream = (project in file("kamon-kafka-stream"))
//  .enablePlugins(JavaAgent)
//  .settings(bintrayPackage := "kamon-kafka")
//  .settings(name := "kamon-kafka-stream")
//  .settings(scalaVersion := "2.12.7")
//  .settings(crossScalaVersions := Seq("2.11.8", "2.12.7"))
//  .settings(javaAgents += "io.kamon"    % "kanela-agent"   % "0.0.400"  % "compile;test")
//  .settings(resolvers += Resolver.bintrayRepo("kamon-io", "snapshots"))
//  .settings(resolvers += Resolver.mavenLocal)
//  .settings(
//        libraryDependencies ++=
//          compileScope(kamonCore, kafkaClient, scalaExtension) ++
//            providedScope(lombok, kafkaSpringTest) ++
//            testScope(kamonTestkit, scalatest, slf4jApi, logbackClassic))
