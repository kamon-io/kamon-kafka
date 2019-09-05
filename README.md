Kafka Integration   ![Build Status](https://travis-ci.org/kamon-io/kamon-kafka.svg?branch=master)
==========================

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/kamon-io/Kamon?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.kamon/kamon-kafka.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.kamon/kamon-kafka_2.12)


The <b>kamon-kafka</b> module requires you to start your application using the Kanela Agent. Kamon will warn you
at startup if you failed to do so.

The bytecode instrumentation provided by the `kamon-kafka` module hooks into the kafka clients to automatically
gather Metrics and start and finish Spans for requests that are issued within a trace. This translates into you having metrics about how
the requests you are doing are behaving.

### Getting Started

Kamon Kafka module is currently available for Scala 2.11 and 2.12.

Supported releases and dependencies are shown below.

| kamon-kafka  | status | jdk  | scala            
|:------:|:------:|:----:|------------------
|  2.0.0 | snapshot | 1.8+ | 2.11, 2.12  

To get started with SBT, simply add the following to your `build.sbt`
file:

```scala
libraryDependencies += "io.kamon" %% "kamon-kafka" % "2.0.0-SNAPSHOT"
```


### Metrics ###

The following metrics will be recorded:

* __TODO__
