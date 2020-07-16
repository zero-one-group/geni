<p align="center">
    <img src="logo/geni.png" width="375px">
</p>

Geni (*/gɜni/* or "gurney" without the r) is a [Clojure](https://clojure.org/) library that wraps [Apache Spark](https://spark.apache.org/). The name means "fire" in Javanese.

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet! See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

[![CI](https://github.com/zero-one-group/geni/workflows/CI/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/commits/develop)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)
[![cljdoc badge](https://cljdoc.org/badge/zero.one/geni)](https://cljdoc.org/d/zero.one/geni/CURRENT)
[![License](https://img.shields.io/github/license/zero-one-group/geni.svg)](LICENSE)

## Overview

Geni is designed to provide an idiomatic Spark interface for Clojure without the hassle of Java or Scala interop. Geni uses Clojure's `->` threading macro as the main way to compose Spark's Dataset and Column operations in place of the usual method chaining in Scala. It also provides a greater degree of dynamism by allowing args of mixed types such as columns, strings and keywords in a single function invocation. See the docs section on [Geni semantics](docs/semantics.md) for more details.

## Resources

* [Contributing Guide](CONTRIBUTING.md)
* [Examples](docs/examples.md)
* [Geni Semantics](docs/semantics.md)
* [Manual Dataset Creation](docs/manual_dataset_creation.md)
* [Optional XGBoost Support](docs/xgboost.md)
* [Using Dataproc](docs/dataproc.md)
* [Why?](docs/why.md)
* [Working with SQL Maps](docs/sql_maps.md)

Alternatively, chat with us on Slack! [![slack](https://badgen.net/badge/-/clojurians%2Fgeni?icon=slack&label)](https://clojurians.slack.com/messages/geni/)

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
; +--------------+-----+
; |Suburb        |count|
; +--------------+-----+
; |Reservoir     |359  |
; |Richmond      |260  |
; |Bentleigh East|249  |
; |Preston       |239  |
; |Brunswick     |222  |
; +--------------+-----+
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

Use [Leiningen](http://leiningen.org/) to create a [template](https://github.com/zero-one-group/geni-template) of a Geni project:

```bash
lein new geni <project-name>
```

[![asciicast](https://asciinema.org/a/346987.svg)](https://asciinema.org/a/346987?speed=1.75)

## Installation

Add the following to your `project.clj` dependency:

[![Clojars Project](https://clojars.org/zero.one/geni/latest-version.svg)](http://clojars.org/zero.one/geni)

You would also need to add Spark as provided dependencies. For instance, have the following key-value pair for the `:profiles` map:

```clojure
:provided
{:dependencies [;; Spark
                [org.apache.spark/spark-avro_2.12 "3.0.0"]
                [org.apache.spark/spark-core_2.12 "3.0.0"]
                [org.apache.spark/spark-hive_2.12 "3.0.0"]
                [org.apache.spark/spark-mllib_2.12 "3.0.0"]
                [org.apache.spark/spark-sql_2.12 "3.0.0"]
                [org.apache.spark/spark-streaming_2.12 "3.0.0"]
                ;; Optional: Spark XGBoost
                [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                [ml.dmlc/xgboost4j_2.12 "1.0.0"]]}
```

You may also need to install `libgomp1` to train XGBoost4j models. When the optional dependencies are not present, the vars to the corresponding functions (such as `ml/xgboost-classifier`) will be left unbound.

## License

Copyright 2020 Zero One Group.

Geni is licensed under Apache License v2.0, see [LICENSE](LICENSE).

## Mentions

Some code was taken from:

* [finagle-clojure](https://github.com/finagle/finagle-clojure) - especially in terms of Scala interop.
* [LispCast](https://lispcast.com/) for [exponential backoff](https://lispcast.com/exponential-backoff/).
* Reddit users [/u/borkdude](https://old.reddit.com/user/borkdude) and [/u/czan](https://old.reddit.com/user/czan) for [with-dynamic-import](src/zero_one/geni/utils.clj).
* StackOverflow user [whocaresanyway's answer](https://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime) for `arg-count`.
