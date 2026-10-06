# Collecting Data from Spark Datasets

Spark functions run on the cluster, but sometimes it's easier to work on the data in plain Clojure. That means moving the data from the Spark workers to the driver, where the Clojure REPL runs, which only works when the data is **small** enough to fit on the driver.

Geni's functions that start with `collect` or `to-` bring the data to the driver. The examples below use the Melbourne housing data in Geni's repo:

```clojure
(require '[zero-one.geni.core :as g])

(def dataframe
  (-> (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet")
      (g/select :Suburb :Address :Rooms :Price :Date)))
```

## A first look

`glimpse` prints a column per line, with its type and its first values, which reads better than `show` when there are many columns:

```clojure
(g/glimpse dataframe {:num-rows 3})
;; =stdout=>
; Rows: at least 3
; Columns: 5
; $ Suburb  <string> "Abbotsford", "Abbotsford", "Abbotsford"
; $ Address <string> "85 Turner St", "25 Bloomburg St", "5 Charles St"
; $ Rooms   <bigint> 2, 2, 3
; $ Price   <double> 1480000.0, 1035000.0, 1465000.0
; $ Date    <string> "3/12/2016", "4/02/2016", "4/03/2017"
```

It counts the rows only when asked, with `:count true`, since that takes a pass over all the data. `to-html` gives Spark's own HTML table, as a notebook shows it, with the cells escaped:

```clojure
(println (g/to-html dataframe {:num-rows 2}))
;; =stdout=>
; <table border='1'>
; <tr><th>Suburb</th><th>Address</th><th>Rooms</th><th>Price</th><th>Date</th></tr>
; <tr><td>Abbotsford</td><td>85 Turner St</td><td>2</td><td>1480000.0</td><td>3/12/2016</td></tr>
; <tr><td>Abbotsford</td><td>25 Bloomburg St</td><td>2</td><td>1035000.0</td><td>4/02/2016</td></tr>
; </table>
; only showing top 2 rows
```

## Collect as Clojure data

A very common case is to access the data as Clojure maps with `collect`:

```clojure
(-> dataframe (g/limit 2) g/collect)
;; => ({:Suburb "Abbotsford",
;;      :Address "85 Turner St",
;;      :Rooms 2,
;;      :Price 1480000.0,
;;      :Date "3/12/2016"}
;;     {:Suburb "Abbotsford",
;;      :Address "25 Bloomburg St",
;;      :Rooms 2,
;;      :Price 1035000.0,
;;      :Date "4/02/2016"})
```

Alternatively, `collect-vals` returns a sequence of vectors:

```clojure
(-> dataframe (g/limit 2) g/collect-vals)
;; => (["Abbotsford" "85 Turner St" 2 1480000.0 "3/12/2016"]
;;     ["Abbotsford" "25 Bloomburg St" 2 1035000.0 "4/02/2016"])
```

To access the values of a single column, use `collect-col`:

```clojure
(-> dataframe (g/limit 2) (g/collect-col :Address))
;; => ("85 Turner St" "25 Bloomburg St")
```

## Collect as a tech.ml.dataset

[tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) keeps a table in columns of primitives and Java objects, and is what [tablecloth](https://github.com/scicloj/tablecloth) and the rest of Clojure's data science libraries build on. With `techascent/tech.ml.dataset` on the classpath, `to-tmd` brings the whole result over as one dataset:

```clojure
(require '[tech.v3.dataset :as ds])

(def housing (-> dataframe (g/limit 5) g/to-tmd))

(ds/column-names housing)
;; => (:Suburb :Address :Rooms :Price :Date)

(vec (housing :Price))
;; => [1480000.0 1035000.0 1465000.0 850000.0 1600000.0]
```

The data comes over as Arrow batches, which the executors make, as they do for PySpark's `toPandas`. That's much quicker than `collect` for a large result, and the columns keep their types:

| Spark | tech.ml.dataset |
|-------|-----------------|
| BOOLEAN and the numbers | primitives, such as `:int64` and `:float64` |
| STRING | `:string` |
| DECIMAL | `:decimal`, of BigDecimals |
| DATE | `:packed-local-date`, which reads as LocalDates |
| TIMESTAMP | `:packed-instant`, which reads as Instants |
| TIMESTAMP_NTZ | `:local-date-time` |
| TIME | `:packed-local-time`, which reads as LocalTimes |
| day-time interval | `:duration`, of Durations, of any length |
| year-month interval | Periods |
| BINARY | byte arrays |
| ARRAY | vectors |
| STRUCT | maps with keyword keys |
| MAP | maps |
| VARIANT | Spark's VariantVal, whose string is the value as JSON |
| MLlib's vectors | what `collect` gives: a vector of doubles for a dense one, and a map for a sparse one |

A null becomes a missing value. Spark's calendar intervals, geometries and geographies have no equivalent, so `to-tmd` throws for them, and so it does for a struct with two fields of one name, which a map can't hold. Columns get keyword names, and `:key-fn` names them otherwise, as `{:key-fn identity}` does with strings. Two columns of one name throw before a job runs, and so do two that `:key-fn` names alike, as `{:key-fn (comp keyword clojure.string/lower-case)}` would `a` and `A`. A dataset holds rows only in its columns, so a result with rows but no columns throws too.

Each column keeps its Spark type, as DDL, in its metadata, which `create-dataframe` uses on the way back, except a column that holds MLlib vectors, whose DDL would be their storage's:

```clojure
(-> housing :Price meta :zero-one.geni/spark-type)
;; => "DOUBLE"
```

### A batch at a time

`stream` reads a result too large for the driver one batch at a time, as a dataset per Arrow batch. A batch has at most `spark.sql.execution.arrow.maxRecordsPerBatch` rows, 10,000 by default, and comes from one partition:

```clojure
(transduce (map ds/row-count) + (g/stream (g/repartition dataframe 4)))
;; => 13580
```

`stream` returns a reducible, so `reduce`, `transduce`, `into` and `run!` read the batches as they go, and stop reading when they're done, when they stop early, as with `(take 2)`, and when they throw. On classic Spark, each partition runs as a job of its own when the reduce gets to it, so only one partition's batches are on the driver at a time. That makes a reduce the way to read it. A reduce is also one of Spark's SQL executions, as a `collect` is, so an observation from `g/observe` gets its metrics when it ends, as it does from `to-tmd`, `to-tensors` and `to-arrow`. It's also seqable, for `seq`, `first` and `doseq`, which read a batch at a time as the seq is realised, but can stop before the end, so close it with `with-open` for those:

```clojure
(with-open [batches (g/stream (g/repartition dataframe 4))]
  (ds/row-count (first batches)))
;; => 3395
```

### Back to Spark

`create-dataframe` takes a dataset. A column that `to-tmd` made, and that still has the datatype that `to-tmd` gave it, gets the Spark type in its metadata, so a round trip keeps the types, a DECIMAL's precision and scale, a map's key type and an interval's fields among them. Selecting, filtering and concatenating rows keep a column's metadata, and replacing its values drops it:

```clojure
(-> housing
    (ds/select-columns [:Suburb :Rooms])
    g/create-dataframe
    g/dtypes)
;; => {:Suburb "StringType", :Rooms "LongType"}
```

Any other column gets its Spark type from its datatype, such as `:int32` INT and `:local-date` DATE, or, for vectors, maps and other objects, from its values, as `records->dataset` infers them, so a map becomes a struct. BigDecimals become a DECIMAL of 38 digits, 18 of them after the point, unless the column's values need more digits before the point or after it. `:schema` gives columns their types, as DataTypes, DDL strings or what `g/->schema` takes:

```clojure
(-> (ds/->dataset {:price [1.5M 2.25M]})
    (g/create-dataframe {:schema {:price "DECIMAL(5, 2)"}})
    g/dtypes)
;; => {:price "DecimalType(5,2)"}
```

A value that its column's type can't hold exactly throws, naming the column, rather than being rounded or becoming a null: a number with more digits than its DECIMAL has room for, a whole number too large for its INT, or a Duration that doesn't fit its interval's fields or range. A double goes into a DECIMAL as its shortest decimal, as Spark's `Decimal` reads one, so 1.5 fits a DECIMAL(12, 2), and `(+ 0.1 0.2)`, which is 0.30000000000000004, doesn't.

## Collect as tensors

`to-tensors` turns columns of numbers into [dtype-next](https://github.com/cnuernber/dtype-next) tensors, which tech.ml.dataset brings, in a map by column name:

```clojure
(require '[tech.v3.tensor :as dtt])

(-> dataframe
    (g/limit 3)
    (g/to-tensors {:columns [:Rooms :Price]})
    :Price
    dtt/->jvm)
;; => [1480000.0 1035000.0 1465000.0]
```

A column of integers (TINYINT, SMALLINT, INT or BIGINT) or floating-point numbers (FLOAT or DOUBLE) with no nulls becomes a tensor of shape [rows], and a column of arrays of those, or of dense MLlib vectors, all of one length, a tensor of shape [rows length], which suits a model's features. Anything else throws, naming the column, DECIMAL among them, which can be cast to DOUBLE first. `stream-tensors` gives a map of tensors per batch, as `stream` gives datasets.

## Collect as Arrow

`to-arrow` gives the Arrow batches themselves, as byte arrays in memory, for other Arrow readers, such as tech.ml.dataset's `tech.v3.libs.arrow` or Python's pyarrow. Each one is a complete Arrow IPC stream, with the schema, one batch and the end marker, so they can't be joined byte for byte:

```clojure
(count (g/to-arrow (g/repartition dataframe 4)))
;; => 4
```

`collect-to-arrow` writes Arrow files instead, which can handle data larger than the driver's heap, as long as the **largest partition** fits on the driver, since the data travels one partition at a time. Repartitioning the data first makes sure of that.

It also needs to know how many rows each Arrow file gets, which should be small enough for each file to fit in the heap, and the directory to write the files to. It writes files of `chunk-size` rows each (the last one can be smaller) and returns their paths:

```clojure
(-> dataframe
    (g/repartition 20)  ;; 20 partitions of about the same size
    (g/collect-to-arrow 1000 "/tmp"))
; ["/tmp/geni12331590604347994819.ipc"
;  "/tmp/geni2107925719499812901.ipc"
;  ...]
```

With enough partitions and a small enough chunk size, data of any size can make it to the driver, although slowly when there's a lot of it. tech.ml.dataset reads the files with `tech.v3.libs.arrow/stream->dataset`, which needs `com.cnuernber/jarrow` on the classpath too.

On Spark 3.5 with JDK 21 or newer, the Arrow functions need Arrow 13 or newer on the classpath (see the [installation notes](../README.md#installation)). Over [Spark Connect](spark_connect.md), the functions that read the batches on the client, `to-tmd`, `stream`, `to-tensors` and `stream-tensors`, need Arrow's own jars, `org.apache.arrow/arrow-vector` and `arrow-memory-netty`, as does `collect-to-arrow`, since the client only has Arrow shaded. `to-arrow`, `glimpse` and `to-html` need nothing more.
