<p align="center">
    <img src="logo/geni.png" width="375px">
</p>

Geni (*/gɜni/* or "gurney" without the r) is a [Clojure](https://clojure.org/) library that wraps [Apache Spark](https://spark.apache.org/). The name means "fire" in Javanese.

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet! See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

[![CI](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/actions)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)
[![License](https://img.shields.io/github/license/zero-one-group/geni.svg)](LICENSE)

## Overview

Geni is designed to provide an idiomatic Spark interface for Clojure without the hassle of Java or Scala interop. Geni uses Clojure's `->` threading macro as the main way to compose Spark's Dataset and Column operations in place of the usual method chaining in Scala. It also provides a greater degree of dynamism by allowing args of mixed types such as columns, strings and keywords in a single function invocation. See the docs section on [Geni semantics](docs/semantics.md) for more details.

## Resources

**Docs:**

* [A Simple Performance Benchmark](docs/simple_performance_benchmark.md)
* [Contributing Guide](CONTRIBUTING.md)
* [Examples](docs/examples.md)
* [Geni Semantics](docs/semantics.md)
* [Manual Dataset Creation](docs/manual_dataset_creation.md)
* [Optional XGBoost Support](docs/xgboost.md)
* [Pandas, NumPy and Other Idioms](docs/pandas_numpy_and_other_idioms.md)
* [Using Dataproc](docs/dataproc.md)
* [Where's The Spark Session?](docs/spark_session.md)
* [Why?](docs/why.md)
* [Working with SQL Maps](docs/sql_maps.md)

**Geni Cookbook:**

0. [Getting Started with Clojure, Geni and Spark](docs/cookbook/part_0_getting_started_with_clojure_geni_and_spark.md)
1. [Reading and Creating Datasets](docs/cookbook/part_1_reading_and_writing_datasets.md)
2. [Selecting Rows and Columns](docs/cookbook/part_2_selecting_rows_and_columns.md)
3. [Grouping and Aggregating](docs/cookbook/part_3_grouping_and_aggregating.md)
4. [Combining Datasets with Joins and Unions](docs/cookbook/part_4_combining_datasets_with_joins_and_unions.md)
5. [String Operations](docs/cookbook/part_5_string_operations.md)
6. [Cleaning up Messy Data](docs/cookbook/part_6_cleaning_up_messy_data.md)
7. [Timestamps and Dates](docs/cookbook/part_7_timestamps_and_dates.md)
8. [Window Functions](docs/cookbook/part_8_window_functions.md)
9. [Reading From and Writing To SQL Databases](docs/cookbook/part_9_reading_from_and_writing_to_sql_databases.md)

[![cljdoc](https://cljdoc.org/badge/zero.one/geni)](https://cljdoc.org/d/zero.one/geni/CURRENT)
[![slack](https://badgen.net/badge/-/clojurians%2Fgeni?icon=slack&label)](https://clojurians.slack.com/messages/geni/)

## Basic Examples

All examples below use the Melbourne housing market data available for free on [Kaggle](https://www.kaggle.com/anthonypino/melbourne-housing-market).

Spark SQL API for data wrangling:

```clojure
(require '[zero-one.geni.core :as g])

(g/count dataframe)
=> 13580

(g/print-schema dataframe)
; root
;  |-- Suburb: string (nullable = true)
;  |-- Address: string (nullable = true)
;  |-- Rooms: long (nullable = true)
;  |-- Type: string (nullable = true)
;  |-- Price: double (nullable = true)
;  |-- Method: string (nullable = true)
;  |-- SellerG: string (nullable = true)
;  |-- Date: string (nullable = true)
;  |-- Distance: double (nullable = true)
;  |-- Postcode: double (nullable = true)
;  |-- Bedroom2: double (nullable = true)
;  |-- Bathroom: double (nullable = true)
;  |-- Car: double (nullable = true)
;  |-- Landsize: double (nullable = true)
;  |-- BuildingArea: double (nullable = true)
;  |-- YearBuilt: double (nullable = true)
;  |-- CouncilArea: string (nullable = true)
;  |-- Lattitude: double (nullable = true)
;  |-- Longtitude: double (nullable = true)
;  |-- Regionname: string (nullable = true)
;  |-- Propertycount: double (nullable = true)

(-> dataframe (g/limit 5) g/show)
; +----------+----------------+-----+----+---------+------+-------+---------+--------+--------+--------+--------+---+--------+------------+---------+-----------+---------+----------+---------------------+-------------+
; |Suburb    |Address         |Rooms|Type|Price    |Method|SellerG|Date     |Distance|Postcode|Bedroom2|Bathroom|Car|Landsize|BuildingArea|YearBuilt|CouncilArea|Lattitude|Longtitude|Regionname           |Propertycount|
; +----------+----------------+-----+----+---------+------+-------+---------+--------+--------+--------+--------+---+--------+------------+---------+-----------+---------+----------+---------------------+-------------+
; |Abbotsford|85 Turner St    |2    |h   |1480000.0|S     |Biggin |3/12/2016|2.5     |3067.0  |2.0     |1.0     |1.0|202.0   |null        |null     |Yarra      |-37.7996 |144.9984  |Northern Metropolitan|4019.0       |
; |Abbotsford|25 Bloomburg St |2    |h   |1035000.0|S     |Biggin |4/02/2016|2.5     |3067.0  |2.0     |1.0     |0.0|156.0   |79.0        |1900.0   |Yarra      |-37.8079 |144.9934  |Northern Metropolitan|4019.0       |
; |Abbotsford|5 Charles St    |3    |h   |1465000.0|SP    |Biggin |4/03/2017|2.5     |3067.0  |3.0     |2.0     |0.0|134.0   |150.0       |1900.0   |Yarra      |-37.8093 |144.9944  |Northern Metropolitan|4019.0       |
; |Abbotsford|40 Federation La|3    |h   |850000.0 |PI    |Biggin |4/03/2017|2.5     |3067.0  |3.0     |2.0     |1.0|94.0    |null        |null     |Yarra      |-37.7969 |144.9969  |Northern Metropolitan|4019.0       |
; |Abbotsford|55a Park St     |4    |h   |1600000.0|VB    |Nelson |4/06/2016|2.5     |3067.0  |3.0     |1.0     |2.0|120.0   |142.0       |2014.0   |Yarra      |-37.8072 |144.9941  |Northern Metropolitan|4019.0       |
; +----------+----------------+-----+----+---------+------+-------+---------+--------+--------+--------+--------+---+--------+------------+---------+-----------+---------+----------+---------------------+-------------+

(-> dataframe (g/describe :Landsize :Rooms :Price) g/show)
; +-------+-----------------+------------------+-----------------+
; |summary|Landsize         |Rooms             |Price            |
; +-------+-----------------+------------------+-----------------+
; |count  |13580            |13580             |13580            |
; |mean   |558.4161266568483|2.9379970544919   |1075684.079455081|
; |stddev |3990.669241109034|0.9557479384215565|639310.7242960163|
; |min    |0.0              |1                 |85000.0          |
; |max    |433014.0         |10                |9000000.0        |
; +-------+-----------------+------------------+-----------------+

(-> dataframe
    (g/group-by :Suburb)
    (g/agg {:count     (g/count "*")
            :n-sellers (g/count-distinct :SellerG)
            :avg-price (g/int (g/mean :Price))})
    (g/order-by (g/desc :count))
    (g/limit 5)
    g/show)
; +--------------+-----+---------+---------+
; |Suburb        |count|n-sellers|avg-price|
; +--------------+-----+---------+---------+
; |Reservoir     |359  |18       |690008   |
; |Richmond      |260  |22       |1083564  |
; |Bentleigh East|249  |21       |1085591  |
; |Preston       |239  |20       |902800   |
; |Brunswick     |222  |21       |1013171  |
; +--------------+-----+---------+---------+

(-> dataframe
    (g/select {:address :Address
               :date    (g/to-date :Date "d/MM/yyyy")
               :coord   (g/struct {:lat :Lattitude :long :Longtitude})})
    g/shuffle
    (g/limit 5)
    g/collect)
=> ({:address "114 Shields St",
     :date #inst "2016-05-21T17:00:00.000-00:00",
     :coord {:lat -37.7847, :long 144.9341}}
    {:address "129 Glenlyon Rd",
     :date #inst "2017-05-05T17:00:00.000-00:00",
     :coord {:lat -37.7723, :long 144.9694}}
    {:address "48 Lyons St",
     :date #inst "2016-04-15T17:00:00.000-00:00",
     :coord {:lat -37.8955, :long 145.0515}}
    {:address "3/31 Clapham St",
     :date #inst "2017-05-19T17:00:00.000-00:00",
     :coord {:lat -37.7549, :long 144.9979}}
    {:address "327 Hull Rd",
     :date #inst "2017-09-08T17:00:00.000-00:00",
     :coord {:lat -37.78329, :long 145.32271}})
```

Spark ML example translated from [Spark's programming guide](https://spark.apache.org/docs/latest/ml-pipeline.html):

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(def training-set
  (g/table->dataset
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

### Install Geni

Install the `geni` script to `/usr/local/bin` with:

```bash
wget https://raw.githubusercontent.com/zero-one-group/geni/develop/scripts/geni
chmod a+x geni
sudo mv geni /usr/local/bin/
```

The command `geni` downloads the latest Geni uberjar and places it in `~/.geni/geni-repl-uberjar.jar`, and runs it with `java -jar`.

### Uberjar

Download the latest Geni REPL uberjar from the [release](https://github.com/zero-one-group/geni/releases) page. Run the uberjar as follows:

```bash
java -jar <uberjar-name>
```

The uberjar app prints the default `SparkSession` instance, starts an nREPL server with an `.nrepl-port` file for easy text-editor connection and steps into a Clojure REPL(-y).

### Leiningen Template

Use [Leiningen](http://leiningen.org/) to create a [template](https://github.com/zero-one-group/geni-template) of a Geni project:

```bash
lein new geni <project-name>
```

`cd` into the project directory and do `lein run`. The templated app runs a Spark ML example, and then steps into a Clojure REPL-y with an `.nrepl-port` file.

### Screencast Demos

<table>
    <tr>
        <th>Install</th>
        <th>Uberjar</th>
        <th>Leiningen</th>
    </tr>
    <tr>
        <td> <a href="https://asciinema.org/a/352552?t=1&theme=monokai&speed=1.75"><img src="https://asciinema.org/a/352552.svg"/></a> </td>
        <td> <a href="https://asciinema.org/a/352138?t=1&theme=monokai&speed=1.75"><img src="https://asciinema.org/a/352138.svg"/></a> </td>
        <td> <a href="https://asciinema.org/a/349721?t=1&theme=monokai&speed=1.75"><img src="https://asciinema.org/a/349721.svg"/></a> </td>
    </tr>
</table>

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
                [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                ;; Databases
                [mysql/mysql-connector-java "8.0.21"]
                [org.postgresql/postgresql "42.2.14"]
                [org.xerial/sqlite-jdbc "3.32.3.1"]
                ;; Optional: Spark XGBoost
                [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                [ml.dmlc/xgboost4j_2.12 "1.0.0"]]}
```

You may also need to install `libatlas3-base` and `libopenblas-base` to use a native BLAS, and install `libgomp1` to train XGBoost4J models. When the optional dependencies are not present, the vars to the corresponding functions (such as `ml/xgboost-classifier`) will be left unbound.

## License

Copyright 2020 Zero One Group.

Geni is licensed under Apache License v2.0, see [LICENSE](LICENSE).

## Mentions

Some code was taken from:

* [finagle-clojure](https://github.com/finagle/finagle-clojure) for Scala interop functions.
* [LispCast](https://lispcast.com/) for [exponential backoff](https://lispcast.com/exponential-backoff/).
* Reddit users [/u/borkdude](https://old.reddit.com/user/borkdude) and [/u/czan](https://old.reddit.com/user/czan) for [with-dynamic-import](src/zero_one/geni/utils.clj).
* StackOverflow user [whocaresanyway's answer](https://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime) for `arg-count`.
* [Julia Evans'](https://jvns.ca/) [Pandas Cookbook](https://github.com/jvns/pandas-cookbook) for its syllabus.
* Reddit user [/u/joinr](https://old.reddit.com/user/joinr) for helping with [unit-testing the REPL](test/zero_one/geni/main_test.clj).
* [Sparkling](https://github.com/gorillalabs/sparkling) for serialisable functions for the RDD API.
