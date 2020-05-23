<img src="logo/geni.png" width="250px">

[![Continuous Integration](https://github.com/zero-one-group/geni/workflows/Continuous%20Integration/badge.svg?branch=develop)](https://github.com/zero-one-group/geni/commits/develop)
[![Code Coverage](https://codecov.io/gh/zero-one-group/geni/branch/develop/graph/badge.svg)](https://codecov.io/gh/zero-one-group/geni)
[![Clojars Project](https://img.shields.io/clojars/v/zero.one/geni.svg)](http://clojars.org/zero.one/geni)

WARNING! This library is still unstable. Some information here may be outdated. Do not use it in production just yet!

See [Flambo](https://github.com/sorenmacbeth/flambo) and [Sparkling](https://github.com/gorillalabs/sparkling) for more mature alternatives.

# Introduction

`geni` (*/gɜni/* or "gurney" without the r) is a Clojure library that wraps Apache Spark. The name comes from the Javanese word for fire.

# Why?

Clojure is particularly well-suited for data wrangling due to its particular focus on fast feedback - most notably through its REPL. Being hosted on the JVM, Clojure interops well with Java (and, by extension, Scala) libaries. However, Spark's pleasant API in Scala becomes quite unidiomatic in Clojure. Geni aims to provide an ergonomic Spark interface for the Clojure REPL.

For instance, instead of making sure the types line up:

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

Geni allows us to write the following instead:

```clojure
(-> dataframe                             ;; Threading macro as the main interface
    (group-by (lower "SellerG") "Suburb") ;; Mix Column and string types
    (agg
      (-> (mean "Price") (as "mean"))     ;; No need to do into-array
      (-> (stddev "Price") (as "std"))    
      (-> (min "Price") (as "min"))
      (-> (max "Price") (as "max")))
    show)
```

Instead of having to deal with interop:

```clojure
(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map
       #(JavaConversions/seqAsJavaList
       (.. % toSeq))))    ;; returns a seq of seqs
                          ;; must zip into map to recover row-like maps
```

Geni's `(collect dataframe)` returns a seq of maps.

Finally, some Column functions such as `+`, `<=` and `&&` become variadic as are expected in any Lisp dialects.

# Examples

Spark SQL API for grouping and aggregating:

```clojure
(require '[zero-one.geni.core :as g])

(-> melbourne-df
    (g/group-by "Suburb")
    (g/agg (-> (g/count "*") (g/as "n")))
    (g/order-by (g/desc "n"))
    g/show)

; +--------------+---+
; |Suburb        |n  |
; +--------------+---+
; |Reservoir     |359|
; |Richmond      |260|
; |Bentleigh East|249|
; |Preston       |239|
; |Brunswick     |222|
; |Essendon      |220|
; |South Yarra   |202|
; |Glen Iris     |195|
; |Hawthorn      |191|
; |Coburg        |190|
; |Northcote     |188|
; |Brighton      |186|
; |Kew           |177|
; |Pascoe Vale   |171|
; |Balwyn North  |171|
; |Yarraville    |164|
; |St Kilda      |162|
; |Glenroy       |159|
; |Port Melbourne|153|
; |Moonee Ponds  |149|
; +--------------+---+
```

MLlib's pipeline:

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
    (ml/tokenizer {:input-col "text"
                   :output-col "words"})
    (ml/hashing-tf {:num-features 1000
                    :input-col "words"
                    :output-col "features"})
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
    (g/select "id" "text" "probability" "prediction")
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

# Geni Semantics: Column Coercion

Many SQL functions and Column methods are overloaded to take either a string or a Column instance as argument. For such cases, Geni implements Column coercion where

1. Column instances are left as they are,
2. strings are interpreted as column names and;
3. other values are interpreted as a literal Column.

Because of this, basic arithmetic operations do not require `lit` wrapping:

```clojure
; The following two expressions are equivalent
(g/- (g// (g/sin Math/PI) (g/cos Math/PI)) (g/tan Math/PI))
(g/- (g// (g/sin (g/lit Math/PI)) (g/cos (g/lit Math/PI))) (g/tan (g/lit Math/PI)))
```

However, string literals do require `lit` wrapping:

```
; The following fails, because "Nelson" is interpreted as a Column
(-> dataframe (g/filter (g/=== "SellerG" "Nelson")))

; The following works, as it checks the column "SellerG" against "Nelson" as a literal
(-> dataframe (g/filter (g/=== "SellerG" (g/lit "Nelson"))))
```

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
