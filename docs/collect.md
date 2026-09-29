# Collecting Data from Spark Datasets

Spark functions run on the cluster, but sometimes it's easier to work on the data in plain Clojure. That means moving the data from the Spark workers to the driver, where the Clojure REPL runs, which only works when the data is **small** enough to fit on the driver.

Geni's functions that start with `collect` bring the data to the driver. The examples below use the Melbourne housing data in Geni's repo:

```clojure
(require '[zero-one.geni.core :as g])

(def dataframe
  (-> (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet")
      (g/select :Suburb :Address :Rooms :Price :Date)))
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

## Collect as Arrow files

`collect-to-arrow` brings the data to the driver as Arrow files instead. It can handle data larger than the driver's heap, as long as the **largest partition** fits on the driver, since the data travels one partition at a time. Repartitioning the data first makes sure of that.

It also needs to know how many rows each Arrow file gets, which should be small enough for each file to fit in the heap, and the directory to write the files to. It writes files of `chunk-size` rows each (the last one can be smaller) and returns their paths:

```clojure
(-> dataframe
    (g/repartition 20)  ;; 20 partitions of about the same size
    (g/collect-to-arrow 1000 "/tmp"))
; ["/tmp/geni12331590604347994819.ipc"
;  "/tmp/geni2107925719499812901.ipc"
;  ...]
```

With enough partitions and a small enough chunk size, data of any size can make it to the driver, although slowly when there's a lot of it.

The files are in the Arrow streaming format, which other tools can read, such as [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) with `tech.v3.libs.arrow/stream->dataset`. On Spark 3.5 with JDK 21 or newer, `collect-to-arrow` needs Arrow 13 or newer on the classpath (see the [installation notes](../README.md#installation)).

## Integration with tech.ml.dataset

tech.ml.dataset also has a [`tech.v3.libs.spark`](https://github.com/techascent/tech.ml.dataset/blob/master/src/tech/v3/libs/spark.clj) namespace, which converts a Spark dataset into a tech.ml.dataset dataset on the driver, and back. The data has to fit in the driver's heap.
