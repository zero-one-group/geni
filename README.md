<img src="logo/geni.png" width="250px">

[![Continuous Integration](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/commits/develop)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet!

See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

# Introduction

`geni` (*/gɜni/* or "gurney" without the r) is a Clojure library that wraps Apache Spark. The name comes from the Javanese word for fire.

See [examples](examples/README.md).

# Why?

This question is probably not directed at the choice of Spark, because it is fairly easy to justify choosing Spark due to its maturity, speed and pleasant API. Rather, why wrap Spark in Clojure when you can use Spark natively in Scala or its popular Python API, PySpark?

Clojure is an excellent programming language for data wrangling due to its particular focus on fast feedbacks - most notably through its REPL. Being hosted on the JVM, Clojure interoperates well with Java (and thus Scala) libaries. However, Spark's pleasant API in Scala becomes quite clunky in Clojure. Geni aims to provide an ergonomic Spark interface for the Clojure REPL.

An example of such nuisance is having to wrap column names inside a Java array of Spark columns:

```clojure
(-> dataframe
    (.groupBy "SellerG" (into-array java.lang.String ["Suburb"]))
    (.agg
      (.as (functions/mean "Price") "mean")
      (into-array Column [(.as (functions/stddev "Price") "std")
                          (.as (functions/min "Price") "min")
                          (.as (functions/max "Price") "max")]))
    .show)
```

Geni aims to provide a Spark interface that plays nice with Clojure's threading macro `->` and dynamic types:

```clojure
(-> dataframe
    (group-by (col "SellerG") "Suburb") ;; Mix Column and string types
    (agg
      (-> (mean "Price") (as "mean"))
      (-> (stddev "Price") (as "std"))  ;; No need to do into-array
      (-> (min "Price") (as "min"))
      (-> (max "Price") (as "max")))
    show)
```

Another inconvenience is having to deal with Scala sequences:

```clojure
(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map
       #(JavaConversions/seqAsJavaList
       (.. % toSeq))))    ;; returns a seq of seqs
                          ;; must zip into map to recover row-like maps
```

In Geni, `(collect dataframe)` returns a vector of maps, where the maps serve a similar purpose to Spark rows.

# Quick Start

Use [Leiningen](http://leiningen.org/) to create a template of a Geni project:

```bash
lein new geni <project-name>
```

Step into the directory, and run the command `lein run`!

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

# Future Work

Features:
- Data-oriented queries and pipeline stages.
- Setup on GCP's Dataproc + guide.
- Clojure docs.

# License

Copyright 2020 Zero One Group.

geni is licensed under Apache License v2.0.

# Mentions

Some code was taken from:

* [finagle-clojure](https://github.com/finagle/finagle-clojure): especially in terms of Scala interop.
