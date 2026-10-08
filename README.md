<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/zero-one-group/geni/develop/assets/geni-lockup-dark.svg">
    <img alt="Geni" src="https://raw.githubusercontent.com/zero-one-group/geni/develop/assets/geni-lockup.svg" width="360">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/zero-one-group/geni/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/zero-one-group/geni/actions/workflows/ci.yml/badge.svg?branch=develop"></a>
  <a href="https://clojars.org/zero.one/geni"><img alt="Clojars" src="https://img.shields.io/clojars/v/zero.one/geni.svg"></a>
  <a href="https://cljdoc.org/d/zero.one/geni/CURRENT"><img alt="cljdoc" src="https://cljdoc.org/badge/zero.one/geni"></a>
  <a href="https://github.com/zero-one-group/geni/blob/develop/LICENSE"><img alt="License" src="https://img.shields.io/github/license/zero-one-group/geni.svg"></a>
</p>

Geni (*/gɜni/* or "gurney" without the r) is a [Clojure](https://clojure.org/) dataframe library that runs on [Apache Spark](https://spark.apache.org/). The name means "fire" in Javanese.

> **Geni is being revived, and 0.4.0 is out, with over 300 more of Spark's SQL functions, results as tech.ml.dataset datasets, tensors and Arrow, and Clojure UDFs over Spark Connect.** The [changelog](CHANGELOG.md) lists what changed since 0.0.42, breaking changes included, and [#359](https://github.com/zero-one-group/geni/issues/359) has the plan for what comes next.

## Overview

Geni provides an idiomatic Spark interface for Clojure without the hassle of Java or Scala interop. It uses Clojure's `->` threading macro to compose Spark's `Dataset` and `Column` operations in place of Scala's method chaining, and it takes columns, strings and keywords alike wherever Spark expects a column. [Geni semantics](docs/semantics.md) has the details.

## Basic examples

The examples below use a 5,000-row sample of the California housing prices data from [Kaggle](https://www.kaggle.com/camnugent/california-housing-prices), which Geni's repo has under `test/resources`.

Spark SQL for data wrangling:

```clojure
(require '[zero-one.geni.core :as g])

(def dataframe (g/read-parquet! "test/resources/housing.parquet"))

(g/count dataframe)
;; => 5000

(g/print-schema dataframe)
;; =stdout=>
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
;; =stdout=>
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
;; =stdout=>
; +-------+------------------+------------------+-----------------+
; |summary|housing_median_age|total_rooms       |population       |
; +-------+------------------+------------------+-----------------+
; |count  |5000              |5000              |5000             |
; |mean   |30.9842           |2393.2132         |1334.9684        |
; |stddev |12.969656616832669|1812.4457510408017|954.0206427949117|
; |min    |1.0               |2.0               |6.0              |
; |max    |52.0              |28258.0           |12203.0          |
; +-------+------------------+------------------+-----------------+

(-> dataframe
    (g/group-by :ocean_proximity)
    (g/agg {:count        (g/count "*")
            :mean-rooms   (g/mean :total_rooms)
            :distinct-lat (g/count-distinct (g/int :latitude))})
    (g/order-by (g/desc :count))
    g/show)
;; =stdout=>
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
;; => ({:ocean "NEAR BAY",
;;      :house {:rooms {:total_rooms 880.0, :total_bedrooms 129.0}, :age 41.0},
;;      :coord {:lat 37.88, :long -122.23}}
;;     {:ocean "NEAR BAY",
;;      :house {:rooms {:total_rooms 7099.0, :total_bedrooms 1106.0}, :age 21.0},
;;      :coord {:lat 37.86, :long -122.22}}
;;     {:ocean "NEAR BAY",
;;      :house {:rooms {:total_rooms 1467.0, :total_bedrooms 190.0}, :age 52.0},
;;      :coord {:lat 37.85, :long -122.24}})
```

Spark ML, with an example from [Spark's programming guide](https://spark.apache.org/docs/latest/ml-pipeline.html):

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
;; =stdout=>
; +---+------------------+----------------------------------------+----------+
; |id |text              |probability                             |prediction|
; +---+------------------+----------------------------------------+----------+
; |4  |spark i j k       |[0.6292098489668484,0.3707901510331516] |0.0       |
; |5  |l m n             |[0.984770006762304,0.015229993237696027]|0.0       |
; |6  |spark hadoop spark|[0.13412348342566116,0.8658765165743388]|1.0       |
; |7  |apache hadoop     |[0.9955732114398529,0.00442678856014711]|0.0       |
; +---+------------------+----------------------------------------+----------+
```

More examples are in the [guides](docs/examples.md) and the [cookbook](#cookbook).

## Installation

Geni is `zero.one/geni` on Clojars. Clojure 1.11 or newer is its only dependency, so Spark comes from your own project. The same jar works with three Spark builds, on JDK 17 or 21:

- Spark 3.5 on Scala 2.12;
- Spark 3.5 on Scala 2.13;
- Spark 4 on Scala 2.13, which also works over [Spark Connect](#spark-connect).

On these JDKs, Spark needs the JVM flags that its own launcher sets. Each setup below is a `deps.edn` alias with Spark's deps and those flags, the same as the alias that Geni's tests run with. This `deps.edn` starts a REPL on Spark 3.5 with `clj -M:spark`:

```edn
{:deps {zero.one/geni {:mvn/version "0.4.0"}}

 :aliases
 {:spark
  {:extra-deps {org.apache.spark/spark-sql_2.12       {:mvn/version "3.5.9"}
                org.apache.spark/spark-mllib_2.12     {:mvn/version "3.5.9"}
                org.apache.spark/spark-avro_2.12      {:mvn/version "3.5.9"}
                org.apache.arrow/arrow-vector       {:mvn/version "13.0.0"}
                org.apache.arrow/arrow-memory-netty {:mvn/version "13.0.0"}}
   :jvm-opts   ["-XX:+IgnoreUnrecognizedVMOptions"
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
                "--add-opens=java.base/java.io=ALL-UNNAMED"
                "--add-opens=java.base/java.net=ALL-UNNAMED"
                "--add-opens=java.base/java.nio=ALL-UNNAMED"
                "--add-opens=java.base/java.util=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
                "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
                "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                "-Djdk.reflect.useDirectMethodHandle=false"]}}}
```

For Spark 3.5 on Scala 2.13, the alias takes the Scala 2.13 builds and the same flags:

```edn
{:aliases
 {:spark-3.5-2.13
  {:extra-deps {org.apache.spark/spark-sql_2.13       {:mvn/version "3.5.9"}
                org.apache.spark/spark-mllib_2.13     {:mvn/version "3.5.9"}
                org.apache.spark/spark-avro_2.13      {:mvn/version "3.5.9"}
                org.apache.arrow/arrow-vector       {:mvn/version "13.0.0"}
                org.apache.arrow/arrow-memory-netty {:mvn/version "13.0.0"}}
   :jvm-opts   ["-XX:+IgnoreUnrecognizedVMOptions"
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
                "--add-opens=java.base/java.io=ALL-UNNAMED"
                "--add-opens=java.base/java.net=ALL-UNNAMED"
                "--add-opens=java.base/java.nio=ALL-UNNAMED"
                "--add-opens=java.base/java.util=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
                "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
                "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                "-Djdk.reflect.useDirectMethodHandle=false"]}}}
```

Spark 4 has flags of its own:

```edn
{:aliases
 {:spark-4
  {:extra-deps {org.apache.spark/spark-sql_2.13      {:mvn/version "4.2.0"}
                org.apache.spark/spark-mllib_2.13    {:mvn/version "4.2.0"}
                org.apache.spark/spark-avro_2.13     {:mvn/version "4.2.0"}
                org.apache.spark/spark-catalyst_2.13 {:mvn/version "4.2.0"}}
   :jvm-opts   ["-XX:+IgnoreUnrecognizedVMOptions"
                "--add-modules=jdk.incubator.vector"
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
                "--add-opens=java.base/java.io=ALL-UNNAMED"
                "--add-opens=java.base/java.net=ALL-UNNAMED"
                "--add-opens=java.base/java.nio=ALL-UNNAMED"
                "--add-opens=java.base/java.util=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
                "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
                "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                "-Dio.netty.tryReflectionSetAccessible=true"
                "-Dio.netty.allocator.type=pooled"
                "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"
                "--sun-misc-unsafe-memory-access=allow"
                "--enable-native-access=ALL-UNNAMED"]}}}
```

A few differences between the setups show up in practice:

- Spark 3.5 ships Arrow 12, which can't allocate buffers on JDK 21 or newer, so `g/collect-to-arrow` fails there. The Arrow 13 deps in the Spark 3.5 aliases fix that, and do no harm on JDK 17.
- Spark 4 turns ANSI mode on by default, so an invalid cast or an overflow throws instead of returning null.
- On JDK 25, Spark's sketch functions, such as `g/hll-sketch-agg`, need `spark-catalyst` ahead of `datasketches-memory` on the classpath. Spark 4.2 ships its own copy of datasketches' JDK check, which takes JDK 25, while datasketches' own copy rejects it. Listing `spark-catalyst` in the setup, as above, puts Spark's copy first.
- On Spark 4, a JDBC write fails when the table doesn't exist yet and Spark has no dialect for the database, as with SQLite. Creating the table first works.

From Leiningen, the same deps go in `:dependencies` (Spark can sit in the `:provided` profile) and the flags in `:jvm-opts`.

Some features need one more dependency: `zero.one/fxl` for `g/read-xlsx!` and `g/write-xlsx!`, XGBoost4J-Spark 3 for `ml/xgboost-classifier` and friends (see [Optional XGBoost Support](docs/xgboost.md)), and a JDBC driver such as `org.xerial/sqlite-jdbc` or `org.postgresql/postgresql` for `g/read-jdbc!` and `g/write-jdbc!`. Without fxl or XGBoost4J-Spark, those functions throw an error that says what to add. Spark ML uses a native BLAS such as OpenBLAS when one is installed.

### A New Project

[deps-new](https://github.com/seancorfield/deps-new) makes a project from Geni's template: a `deps.edn` with the `:spark` and `:spark-4` setups above, a small word-count app and its test, and a `build.clj` whose uberjar is ready for `spark-submit`, with the app and Geni compiled ahead of time and without Spark. With the Clojure CLI 1.12 or newer:

```bash
clojure -Ttools install-latest :lib io.github.seancorfield/deps-new :as new
clojure -Tnew create :template io.github.zero-one-group/geni%template%zero-one/geni :name acme/spark-app
```

The new project's README has the commands that run, test and build it.

### Spark Connect

With Spark 4's Spark Connect client in place of Spark, Geni sends its queries to a Spark Connect server, such as one on a cluster or on Databricks, and `g/connect` starts the session. The client can't share a classpath with `spark-sql`, so it's a setup of its own, with Spark 4's JVM flags:

```edn
{:aliases
 {:spark-connect
  {:extra-deps {org.apache.spark/spark-connect-client-jvm_2.13 {:mvn/version "4.2.0"}}
   :jvm-opts   ["-XX:+IgnoreUnrecognizedVMOptions"
                "--add-modules=jdk.incubator.vector"
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
                "--add-opens=java.base/java.io=ALL-UNNAMED"
                "--add-opens=java.base/java.net=ALL-UNNAMED"
                "--add-opens=java.base/java.nio=ALL-UNNAMED"
                "--add-opens=java.base/java.util=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
                "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
                "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                "-Dio.netty.tryReflectionSetAccessible=true"
                "-Dio.netty.allocator.type=pooled"
                "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"
                "--sun-misc-unsafe-memory-access=allow"
                "--enable-native-access=ALL-UNNAMED"]}}}
```

RDDs, MLlib and the other parts that need a SparkContext stay with classic Spark. The [Spark Connect guide](docs/spark_connect.md) has the details.

## The Geni CLI

The Geni CLI is an uberjar with Geni, Spark 3.5 and a REPL. It starts a Spark session and an nREPL server, writes an `.nrepl-port` file for your editor, and drops into a REPL with Geni's namespaces required. The uberjar is on the [0.4.0 release](https://github.com/zero-one-group/geni/releases/tag/v0.4.0), and runs on JDK 17 or 21:

```bash
curl -fLO https://github.com/zero-one-group/geni/releases/download/v0.4.0/geni-repl-uberjar-0.4.0.jar
java -jar geni-repl-uberjar-0.4.0.jar
```

Its manifest carries the JVM flags that Spark needs, and it logs at WARN, as `spark-shell` does. Given a file, as in `java -jar geni-repl-uberjar-0.4.0.jar script.clj`, it runs the file instead of the REPL.

The `geni` script downloads the uberjar of the latest stable release to `~/.geni`, and runs it:

```bash
curl -fLO https://raw.githubusercontent.com/zero-one-group/geni/develop/scripts/geni
chmod a+x geni
sudo mv geni /usr/local/bin/
geni
```

A `geni` script installed before 0.1.0 doesn't notice new releases, and keeps the uberjar it downloaded first. Install it again with the commands above, or run `geni --force-download` once after each release.

## Docs

The guides:

- [Why?](docs/why.md)
- [Design goals](docs/design_goals.md)
- [Geni semantics](docs/semantics.md)
- [Where's the Spark session?](docs/spark_session.md)
- [Examples](docs/examples.md)
- [Creating Spark schemas](docs/creating_spark_schemas.md)
- [Manual dataset creation](docs/manual_dataset_creation.md)
- [Working with SQL maps](docs/sql_maps.md)
- [Clojure UDFs](docs/udfs.md)
- [Collecting data from Spark datasets](docs/collect.md)
- [Pandas, NumPy and other idioms](docs/pandas_numpy_and_other_idioms.md)
- [Optional XGBoost support](docs/xgboost.md)
- [Graphs with GraphFrames](docs/graphs.md)
- [Spark Connect](docs/spark_connect.md)
- [Using Kubernetes](docs/kubernetes_basic.md), written for Spark 3.0
- [A simple performance benchmark](docs/simple_performance_benchmark.md), from 2020

The code in the README and the guides runs as tests on every pull request, and the cookbook runs every week. The API reference is on [cljdoc](https://cljdoc.org/d/zero.one/geni/CURRENT). Questions are welcome in [#geni](https://clojurians.slack.com/messages/geni/) on the Clojurians Slack, and on [Zulip](https://clojurians.zulipchat.com/#narrow/stream/256615-geni).

### Cookbook

The cookbook follows the syllabus of Julia Evans' [Pandas Cookbook](https://github.com/jvns/pandas-cookbook):

0. [Getting started with Clojure, Geni and Spark](docs/cookbook/part_00_getting_started_with_clojure_geni_and_spark.md)
1. [Reading and writing datasets](docs/cookbook/part_01_reading_and_writing_datasets.md)
2. [Selecting rows and columns](docs/cookbook/part_02_selecting_rows_and_columns.md)
3. [Grouping and aggregating](docs/cookbook/part_03_grouping_and_aggregating.md)
4. [Combining datasets with joins and unions](docs/cookbook/part_04_combining_datasets_with_joins_and_unions.md)
5. [String operations](docs/cookbook/part_05_string_operations.md)
6. [Cleaning up messy data](docs/cookbook/part_06_cleaning_up_messy_data.md)
7. [Timestamps and dates](docs/cookbook/part_07_timestamps_and_dates.md)
8. [Window functions](docs/cookbook/part_08_window_functions.md)
9. [Reading from and writing to SQL databases](docs/cookbook/part_09_reading_from_and_writing_to_sql_databases.md)
10. [Avoiding repeated computations with caching](docs/cookbook/part_10_avoiding_repeated_computations_with_caching.md)
11. [Basic ML pipelines](docs/cookbook/part_11_basic_ml_pipelines.md)
12. [Customer segmentation with NMF](docs/cookbook/part_12_customer_segmentation_with_nmf.md)
13. [Text classification with Spark NLP](docs/cookbook/part_13_text_classification_with_spark_nlp.md)

## Contributing

Bug reports, docs and code are all welcome: see the [contributing guide](CONTRIBUTING.md) and the [code of conduct](CODE_OF_CONDUCT.md).

## How the revival was built

**Claude (Anthropic) wrote most of the code, tests and docs of the 2026 revival**, from the move to the Clojure CLI onwards. The maintainer set the scope, made the design calls, ran every check and reviewed the result. Geni up to 0.0.42 was written without it.

What holds the work up is the checking: the test suite runs on every supported Spark and JDK, and every example in the README and the guides runs as a test.

## License

Copyright 2020 Zero One Group.

Geni is licensed under Apache License v2.0, see [LICENSE](LICENSE).

## Mentions

Some parts of the project have been taken from or inspired by:

* [finagle-clojure](https://github.com/finagle/finagle-clojure) for Scala interop functions.
* Reddit users [/u/borkdude](https://old.reddit.com/user/borkdude) and [/u/czan](https://old.reddit.com/user/czan) for [with-dynamic-import](src/clojure/zero_one/geni/utils.clj).
* StackOverflow user [whocaresanyway's answer](https://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime) for `arg-count`.
* [Julia Evans'](https://jvns.ca/) [Pandas Cookbook](https://github.com/jvns/pandas-cookbook) for its syllabus.
* Reddit user [/u/joinr](https://old.reddit.com/user/joinr) for helping with [unit-testing the REPL](cli/test/zero_one/geni/main_test.clj).
* [Sparkling](https://github.com/gorillalabs/sparkling), [sparkplug](https://github.com/amperity/sparkplug) and [Gabriel Borges](https://github.com/borgesgabriel) for helping with the RDD function serialisation.
* [Chris Nuernberger](https://github.com/cnuernber) and [Tomasz Sulej](https://github.com/tsulej) for helping with [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) and [tablecloth](https://github.com/scicloj/tablecloth).
* [Ubuntu](https://ubuntu.com/community/code-of-conduct), [Django](https://www.djangoproject.com/conduct/) and [Conjure](https://github.com/Olical/conjure/blob/master/.github/CODE_OF_CONDUCT.md) for their codes of conduct.
* [FZF](https://github.com/junegunn/fzf) for their issue template.
