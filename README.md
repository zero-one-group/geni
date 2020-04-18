<img src="logo/geni.png" width="250px">

[![Continuous Integration](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/commits/develop)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet!

See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

# Introduction

`geni` (*/gɜni/* or "gurney" without the r) is a Clojure library that wraps Apache Spark. The name comes from the Javanese word for fire.

# Why?

This question is probably not directed at the choice of Spark, because it is fairly easy to justify choosing Spark due to its maturity, speed and pleasant API. Rather, why wrap Spark in Clojure when you can use Spark natively in Scala or its popular Python API, PySpark?

Clojure is an excellent programming language for data wrangling due to its particular focus on fast feedbacks - most notably through its REPL. Being hosted on the JVM, Clojure interoperates well with Java (and thus Scala) libaries. However, although Spark's API is pleasant to use in Scala, it becomes quite clunky in Clojure.

An example of a nuisance is having to wrap column names inside a Java array of Spark columns:

```clojure
(-> dataframe
    (.groupBy "SellerG" (into-array java.lang.String ["Suburb"]))
    (.agg
      (.as (functions/mean "Price") "mean")
      (into-array Column [(.as (functions/stddev "Price") "std")
                          (.as (functions/min "Price") "min")
                          (.as (functions/max "Price") "max")])))
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

Another example is having to deal with Scala sequences:

```clojure
(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map #(into
             []
             (JavaConversions/seqAsJavaList (.. % toSeq))))) ;; returns a lazy seq of vectors
```

with Geni, `(collect dataframe)` returns a vector of maps, where the maps serve a similar purpose to Spark rows.

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
