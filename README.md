<p align="center">
    <img src="logo/geni.png" width="375px">
</p>

Geni (*/gɜni/* or "gurney" without the r) is a [Clojure](https://clojure.org/) library that wraps [Apache Spark](https://spark.apache.org/). The name means "fire" in Javanese.

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet! See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

[![Continuous Integration](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/commits/develop)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)
[![License](https://img.shields.io/github/license/zero-one-group/geni.svg)](license.txt)

## Overview

Geni is designed to provide an idiomatic Spark interface for Clojure without the hassle of Java or Scala interop. Geni relies on Clojure's `->` threading macro as the main way to compose Spark's Dataset and Column operations in place of the usual method chaining in Scala. It also provides a greater degree of dynamism by allowing args of mixed types such as columns, strings and keywords in a single function invocation. See the docs section on [Geni semantics](docs/semantics.md) for more details.

## Why?

Many data tasks such as exploratory data analysis require frequent feedback from the data. For such tasks, the process can typically be described by the following loop i) question about the data, ii) transformation of the data, iii) interpretation of the results of the transformation and iv) new questions about the data.

Geni optimises for the speed of this feedback loop by providing a dynamic and terse interface that complements working in the Clojure REPL.

For many Geni functions, we do not need to make sure that the types line up; only that the args can be converted into Spark Columns. Consider the following example:

```clojure
(-> dataframe
    (group-by (lower "SellerG")  ;; Mixed Column, string and keyword types.
              "Suburb"           ;; No need for `into-array`.
              :Regionname)
    (agg {:mean (mean :Price)    ;; Map keys are interpreted as aliases.
          :std  (stddev :Price)
          :min  (min :Price)
          :max  (max :Price)})
    show)
```

In contrast, we would have to write the following with pure interop:

```clojure
(-> dataframe
    (.groupBy (into-array Column [(functions/lower (functions/col "SellerG"))
                                  (functions/col "Suburb")
                                  (functions/col "Regionname")]))
    (.agg
      (.as (functions/mean "Price") "mean")
      (into-array Column [(.as (functions/stddev "Price") "std")
                          (.as (functions/min "Price") "min")
                          (.as (functions/max "Price") "max")]))
    .show)
```

At times, it can be tricky to figure out the interop , which often times requires careful inspection of Java reflection. This problem is compounded in the case of Scala interop:

```clojure
(import '(scala.collection JavaConversions))

(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map #(JavaConversions/seqAsJavaList (.. % toSeq))))
     ;; returns a seq of seqs - must zipmap with col names to get maps
```

Geni handles all the interop in the background - `(collect dataframe)` returns a seq of maps, where the keys are keywordised and nested structs are collected as nested maps. Collecting deeply nested structs as maps becomes straightforward:

```clojure
(-> dataframe
    (select
      {:property
       (struct
         {:market   (struct :SellerG :Price :Date)
          :house    (struct :Landsize :Rooms)
          :location (struct :Address {:coord (struct :Lattitude :Longtitude)})})})
    (limit 1)
    collect)
=> ({:property
     {:market {:SellerG "Biggin", :Price 1480000.0, :Date "3/12/2016"},
      :house {:Landsize 202.0, :Rooms 2},
      :location
      {:Suburb "Abbotsford",
       :Address "85 Turner St",
       :coord {:Lattitude -37.7996, :Longtitude 144.9984}}}})
```

Finally, Geni supports various Clojure (or Lisp) idioms by making some functions variadic (`+`, `<=`, `&&`, etc.) and providing functions with Clojure analogues that are not available in Spark such as `remove`. For example:

```clojure
(-> dataframe
    (remove (like :Regionname "%Metropolitan%"))
    (filter (&& (< 2 :Rooms 5)
                (< 5e5 :Price 6e5)
                (< :YearBuilt 2010)))
    (select :Regionname :Rooms :Price :YearBuilt)
    show)
```

Note that functions such as `remove` and `filter` accept the Spark Dataset in the first argument. This unfortunate departure from Clojure's idioms is necessary to emulate Scala's method chaining with the threading macro `->`.

## Basic Examples

Spark SQL API for grouping and aggregating:

```clojure
(require '[zero-one.geni.core :as g])

(-> dataframe
    (g/group-by :Suburb)
    g/count
    (g/order-by (g/desc :count))
    (g/limit 5)
    g/show)
; +--------------+---+
; |Suburb        |n  |
; +--------------+---+
; |Reservoir     |359|
; |Richmond      |260|
; |Bentleigh East|249|
; |Preston       |239|
; |Brunswick     |222|
; +--------------+---+
```

Spark ML example translated from [Spark's programming guide](https://spark.apache.org/docs/latest/ml-pipeline.html):

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(def training-set
  (g/table->dataset
    spark
    [[0 "a b c d e spark"  1.0]
     [1 "b d"              0.0]
     [2 "spark f g h"      1.0]
     [3 "hadoop mapreduce" 0.0]]
    [:id :text :label]))

(def pipeline
  (ml/pipeline
    (ml/tokenizer {:input-col :text
                   :output-col :words})
    (ml/hashing-tf {:num-features 1000
                    :input-col :words
                    :output-col :features})
    (ml/logistic-regression {:max-iter 10
                             :reg-param 0.001})))

(def model (ml/fit training-set pipeline))

(def test-set
  (g/table->dataset
    spark
    [[4 "spark i j k"]
     [5 "l m n"]
     [6 "spark hadoop spark"]
     [7 "apache hadoop"]]
    [:id :text]))

(-> test-set
    (ml/transform model)
    (g/select :id :text :probability :prediction)
    g/show)
;; +---+------------------+----------------------------------------+----------+
;; |id |text              |probability                             |prediction|
;; +---+------------------+----------------------------------------+----------+
;; |4  |spark i j k       |[0.1596407738787411,0.8403592261212589] |1.0       |
;; |5  |l m n             |[0.8378325685476612,0.16216743145233883]|0.0       |
;; |6  |spark hadoop spark|[0.0692663313297627,0.9307336686702373] |1.0       |
;; |7  |apache hadoop     |[0.9821575333444208,0.01784246665557917]|0.0       |
;; +---+------------------+----------------------------------------+----------+
```

More detailed examples can be found [here](examples/README.md).There is also a one-to-one walkthrough of Chapter 5 of NVIDIA's [Accelerating Apache Spark 3.x](https://www.nvidia.com/en-us/deep-learning-ai/solutions/data-science/apache-spark-3/ebook-sign-up/), which can be found [here](examples/nvidia_pipeline.clj).

## Quick Start

Use [Leiningen](http://leiningen.org/) to create a template of a Geni project:

```bash
lein new geni <project-name>
```

Step into the directory, and run the command `lein run`!

## Installation

Note that Geni wraps Apache Spark 3.0.0, which uses Scala 2.12, which has [incomplete support for JDK 11](https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html). JDK 8 is recommended.

Add the following to your `project.clj` dependency:

[![Clojars Project](https://clojars.org/zero.one/geni/latest-version.svg)](http://clojars.org/zero.one/geni)

You would also need to add Spark as provided dependencies. For instance, have the following key-value pair for the `:profiles` map:

```clojure
:provided
{:dependencies [;; Spark
                [org.apache.spark/spark-core_2.12 "3.0.0"]
                [org.apache.spark/spark-hive_2.12 "3.0.0"]
                [org.apache.spark/spark-mllib_2.12 "3.0.0"]
                [org.apache.spark/spark-sql_2.12 "3.0.0"]
                [org.apache.spark/spark-streaming_2.12 "3.0.0"]
                ;; Optional: Spark XGBoost
                [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                [ml.dmlc/xgboost4j_2.12 "1.0.0"]
                ;; Optional: Google Sheets Integration
                [com.google.api-client/google-api-client "1.30.9"]
                [com.google.apis/google-api-services-drive "v3-rev197-1.25.0"]
                [com.google.apis/google-api-services-sheets "v4-rev612-1.25.0"]
                [com.google.oauth-client/google-oauth-client-jetty "1.30.6"]
                [org.apache.hadoop/hadoop-client "2.7.3"]]}
```

When the optional dependencies are not present, the vars to the corresponding functions (such as `ml/xgboost-classifier` and `g/read-sheets`) will be left unbound.

## Further Resources

* [Examples](docs/examples.md)
* [Geni Semantics](docs/semantics.md)
* [Optional Google-Sheets Integration](docs/google_sheets.md)
* [Optional XGBoost Support](docs/xgboost.md)

## License

Copyright 2020 Zero One Group.

Geni is licensed under Apache License v2.0.

## Mentions

Some code was taken from:

* [finagle-clojure](https://github.com/finagle/finagle-clojure) - especially in terms of Scala interop.
* [LispCast](https://lispcast.com/) for [exponential backoff](https://lispcast.com/exponential-backoff/).
* Reddit users [/u/borkdude](https://old.reddit.com/user/borkdude) and [/u/czan](https://old.reddit.com/user/czan) for [with-dynamic-import](src/zero_one/geni/utils.clj).
