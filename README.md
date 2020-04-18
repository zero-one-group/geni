<img src="logo/geni.png" width="250px">

[![pipeline status](https://gitlab.com/zero-one-open-source/geni/badges/develop/pipeline.svg)](https://gitlab.com/zero-one-open-source/geni/-/commits/develop)
[![coverage report](https://gitlab.com/zero-one-open-source/geni/badges/develop/coverage.svg)](https://gitlab.com/zero-one-open-source/geni/-/commits/develop)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet!

See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

[[_TOC_]]

# Introduction

`geni` (*/gɜni/* or "gurney" without the r) is a Clojure library that wraps Apache Spark. The name comes from the Javanese word for fire.

# Installation

Note that `geni` wraps Apache Spark 2.4.5, which uses Scala 2.12, which has [incomplete support for JDK 11](https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html). JDK 8 is recommended.

Add the following to your `project.clj` dependency:

[![Clojars Project](https://clojars.org/zero.one/geni/latest-version.svg)](http://clojars.org/zero.one/geni)

You would also need to add Spark as provided dependencies. For instance, have the following key-value pair for the `:profiles` map:

```clojure
:provided
{:dependencies [[org.apache.spark/spark-core_2.12 "2.4.5"]
                [org.apache.spark/spark-hive_2.12 "2.4.5"]
                [org.apache.spark/spark-mllib_2.12 "2.4.5"]
                [org.apache.spark/spark-sql_2.12 "2.4.5"]
                [org.apache.spark/spark-streaming_2.12 "2.4.5"]]}
```

# License

Copyright 2020 Zero One Group.

geni is licensed under Apache License v2.0.
