<p align="center">
    <img src="logo/geni.png" width="375px">
</p>

Geni (*/gɜni/* or "gurney" without the r) is a [Clojure](https://clojure.org/) dataframe library that runs on [Apache Spark](https://spark.apache.org/). The name means "fire" in Javanese.

[![CI](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/actions)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)
[![License](https://img.shields.io/github/license/zero-one-group/geni.svg)](LICENSE)

## Overview

Geni provides an idiomatic Spark interface for Clojure without the hassle of Java or Scala interop. Geni uses Clojure's `->` threading macro as the main way to compose Spark's `Dataset` and `Column` operations in place of the usual method chaining in Scala. It also provides a greater degree of dynamism by allowing args of mixed types such as columns, strings and keywords in a single function invocation. See the docs section on [Geni semantics](docs/semantics.md) for more details.

## Resources

<table>
  <tbody>
    <tr>
      <th align="center" width="441">
        Docs
      </th>
      <th align="center" width="441">
        Cookbook
      </th>
    </tr>
    <tr>
      <td>
        <ul>
            <li><a href="docs/simple_performance_benchmark.md">A Simple Performance Benchmark</a></li>
            <li><a href="CODE_OF_CONDUCT.md">Code of Conduct</a></li>
            <li><a href="CONTRIBUTING.md">Contributing Guide</a></li>
            <li><a href="docs/creating_spark_schemas.md">Creating Spark Schemas</a></li>
            <li><a href="docs/examples.md">Examples</a></li>
            <li><a href="docs/design_goals.md">Design Goals</a></li>
            <li><a href="docs/semantics.md">Geni Semantics</a></li>
            <li><a href="docs/manual_dataset_creation.md">Manual Dataset Creation</a></li>
            <li><a href="docs/xgboost.md">Optional XGBoost Support</a></li>
            <li><a href="docs/pandas_numpy_and_other_idioms.md">Pandas, NumPy and Other Idioms</a></li>
            <li><a href="docs/dataproc.md">Using Dataproc</a></li>
            <li><a href="docs/kubernetes_basic.md">Using Kubernetes</a></li>
            <li><a href="docs/spark_session.md">Where's The Spark Session</a></li>
            <li><a href="docs/why.md">Why?</a></li>
            <li><a href="docs/sql_maps.md">Working with SQL Maps</a></li>
            <li><a href="docs/collect.md">Collecting Data from Spark Datasets</a></li>
        </ul>
      </td>
      <td>
        <ol start="0">
            <li><a href="docs/cookbook/part_00_getting_started_with_clojure_geni_and_spark.md">
                Getting Started with Clojure, Geni and Spark
            </a></li>
            <li><a href="docs/cookbook/part_01_reading_and_writing_datasets.md">
                Reading and Writing Datasets
            </a></li>
            <li><a href="docs/cookbook/part_02_selecting_rows_and_columns.md">
                Selecting Rows and Columns
            </a></li>
            <li><a href="docs/cookbook/part_03_grouping_and_aggregating.md">
                Grouping and Aggregating
            </a></li>
            <li><a href="docs/cookbook/part_04_combining_datasets_with_joins_and_unions.md">
                Combining Datasets with Joins and Unions
            </a></li>
            <li><a href="docs/cookbook/part_05_string_operations.md">
                String Operations
            </a></li>
            <li><a href="docs/cookbook/part_06_cleaning_up_messy_data.md">
                Cleaning up Messy Data
            </a></li>
            <li><a href="docs/cookbook/part_07_timestamps_and_dates.md">
                Timestamps and Dates
            </a></li>
            <li><a href="docs/cookbook/part_08_window_functions.md">
                Window Functions
            </a></li>
            <li><a href="docs/cookbook/part_09_reading_from_and_writing_to_sql_databases.md">
                Reading from and Writing to SQL Databases
            </a></li>
            <li><a href="docs/cookbook/part_10_avoiding_repeated_computations_with_caching.md">
                Avoiding Repeated Computations with Caching
            </a></li>
            <li><a href="docs/cookbook/part_11_basic_ml_pipelines.md">
                Basic ML Pipelines
            </a></li>
            <li><a href="docs/cookbook/part_12_customer_segmentation_with_nmf.md">
                Customer Segmentation with NMF
            </a></li>
        </ol>
      </td>
    </tr>
  </tbody>
</table>


[![cljdoc](https://cljdoc.org/badge/zero.one/geni)](https://cljdoc.org/d/zero.one/geni/CURRENT)
[![slack](https://badgen.net/badge/-/clojurians%2Fgeni?icon=slack&label)](https://clojurians.slack.com/messages/geni/)
[![zulip](https://img.shields.io/badge/zulip-clojurians%2Fgeni-brightgreen.svg)](https://clojurians.zulipchat.com/#narrow/stream/256615-geni)

## Basic Examples

All examples below use the Statlib California housing prices data available for free on [Kaggle](https://www.kaggle.com/camnugent/california-housing-prices).

Spark SQL API for data wrangling:

```clojure
(require '[zero-one.geni.core :as g])

(def dataframe (g/read-parquet! "test/resources/housing.parquet"))

(g/count dataframe)
=> 5000

(g/print-schema dataframe)
; root
;  |-- longitude: double (nullable = true)
;  |-- latitude: double (nullable = true)
;  |-- housing_median_age: double (nullable = true)
;  |-- total_rooms: double (nullable = true)
;  |-- total_bedrooms: double (nullable = true)
;  |-- population: double (nullable = true)
;  |-- households: double (nullable = true)
;  |-- median_income: double (nullable = true)
;  |-- median_house_value: double (nullable = true)
;  |-- ocean_proximity: string (nullable = true)

(-> dataframe (g/limit 5) g/show)
; +---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+
; |longitude|latitude|housing_median_age|total_rooms|total_bedrooms|population|households|median_income|median_house_value|ocean_proximity|
; +---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+
; |-122.23  |37.88   |41.0              |880.0      |129.0         |322.0     |126.0     |8.3252       |452600.0          |NEAR BAY       |
; |-122.22  |37.86   |21.0              |7099.0     |1106.0        |2401.0    |1138.0    |8.3014       |358500.0          |NEAR BAY       |
; |-122.24  |37.85   |52.0              |1467.0     |190.0         |496.0     |177.0     |7.2574       |352100.0          |NEAR BAY       |
; |-122.25  |37.85   |52.0              |1274.0     |235.0         |558.0     |219.0     |5.6431       |341300.0          |NEAR BAY       |
; |-122.25  |37.85   |52.0              |1627.0     |280.0         |565.0     |259.0     |3.8462       |342200.0          |NEAR BAY       |
; +---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+

(-> dataframe (g/describe :housing_median_age :total_rooms :population) g/show)
; +-------+------------------+------------------+-----------------+
; |summary|housing_median_age|total_rooms       |population       |
; +-------+------------------+------------------+-----------------+
; |count  |5000              |5000              |5000             |
; |mean   |30.9842           |2393.2132         |1334.9684        |
; |stddev |12.969656616832669|1812.4457510408017|954.0206427949117|
; |min    |1.0               |1000.0            |100.0            |
; |max    |9.0               |999.0             |999.0            |
; +-------+------------------+------------------+-----------------+

(-> dataframe
    (g/group-by :ocean_proximity)
    (g/agg {:count        (g/count "*")
            :mean-rooms   (g/mean :total_rooms)
            :distinct-lat (g/count-distinct (g/int :latitude))})
    (g/order-by (g/desc :count))
    g/show)
; +---------------+-----+------------------+------------+
; |ocean_proximity|count|mean-rooms        |distinct-lat|
; +---------------+-----+------------------+------------+
; |INLAND         |1823 |2358.181020296215 |10          |
; |<1H OCEAN      |1783 |2467.5361749859785|7           |
; |NEAR BAY       |1287 |2368.72027972028  |2           |
; |NEAR OCEAN     |107  |2046.1869158878505|2           |
; +---------------+-----+------------------+------------+

(-> dataframe
    (g/select {:ocean :ocean_proximity
               :house (g/struct {:rooms (g/struct :total_rooms :total_bedrooms)
                                 :age   :housing_median_age})
               :coord (g/struct {:lat :latitude :long :longitude})})
    (g/limit 3)
    g/collect)
=> ({:ocean "NEAR BAY",
     :house {:rooms {:total_rooms 880.0, :total_bedrooms 129.0}, 
             :age 41.0},
     :coord {:lat 37.88, :long -122.23}}
    {:ocean "NEAR BAY",
     :house {:rooms {:total_rooms 7099.0, :total_bedrooms 1106.0}, 
             :age 21.0},
     :coord {:lat 37.86, :long -122.22}}
    {:ocean "NEAR BAY",
     :house {:rooms {:total_rooms 1467.0, :total_bedrooms 190.0}, 
             :age 52.0},
     :coord {:lat 37.85, :long -122.24}})
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

More detailed examples can be found [here](examples/README.md).

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
                [org.apache.spark/spark-avro_2.12 "3.1.1"]
                [org.apache.spark/spark-core_2.12 "3.1.1"]
                [org.apache.spark/spark-hive_2.12 "3.1.1"]
                [org.apache.spark/spark-mllib_2.12 "3.1.1"]
                [org.apache.spark/spark-sql_2.12 "3.1.1"]
                [org.apache.spark/spark-streaming_2.12 "3.1.1"]
                [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                ; Arrow
                [org.apache.arrow/arrow-memory-netty "2.0.0"]
                [org.apache.arrow/arrow-memory-core "2.0.0"]
                [org.apache.arrow/arrow-vector "2.0.0"
                :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
                ;; Databases
                [mysql/mysql-connector-java "8.0.23"]
                [org.postgresql/postgresql "42.2.19"]
                [org.xerial/sqlite-jdbc "3.34.0"]
                ;; Optional: Spark XGBoost
                [ml.dmlc/xgboost4j-spark_2.12 "1.2.0"]
                [ml.dmlc/xgboost4j_2.12 "1.2.0"]]}
```

You may also need to install `libatlas3-base` and `libopenblas-base` to use a native BLAS, and install `libgomp1` to train XGBoost4J models. When the optional dependencies are not present, the vars to the corresponding functions (such as `ml/xgboost-classifier`) will be left unbound.

## License

Copyright 2020 Zero One Group.

Geni is licensed under Apache License v2.0, see [LICENSE](LICENSE).

## Mentions

Some parts of the project have been taken from or inspired by:

* [finagle-clojure](https://github.com/finagle/finagle-clojure) for Scala interop functions.
* [LispCast](https://lispcast.com/) for [exponential backoff](https://lispcast.com/exponential-backoff/).
* Reddit users [/u/borkdude](https://old.reddit.com/user/borkdude) and [/u/czan](https://old.reddit.com/user/czan) for [with-dynamic-import](src/zero_one/geni/utils.clj).
* StackOverflow user [whocaresanyway's answer](https://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime) for `arg-count`.
* [Julia Evans'](https://jvns.ca/) [Pandas Cookbook](https://github.com/jvns/pandas-cookbook) for its syllabus.
* Reddit user [/u/joinr](https://old.reddit.com/user/joinr) for helping with [unit-testing the REPL](test/zero_one/geni/main_test.clj).
* [Sparkling](https://github.com/gorillalabs/sparkling), [sparkplug](https://github.com/amperity/sparkplug) and [Gabriel Borges](https://github.com/borgesgabriel) for helping with the RDD function serialisation.
* [Chris Nuernberger](https://github.com/cnuernber) and [Tomasz Sulej](https://github.com/tsulej) for helping with [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) and [tablecloth](https://github.com/scicloj/tablecloth).
* [Ubuntu](https://ubuntu.com/community/code-of-conduct), [Django](https://www.djangoproject.com/conduct/) and [Conjure](https://github.com/Olical/conjure/blob/master/.github/CODE_OF_CONDUCT.md) for their codes of conduct.
* [FZF](https://github.com/junegunn/fzf) for their issue template.
