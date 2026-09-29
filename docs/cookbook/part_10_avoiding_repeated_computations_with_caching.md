# CB-10: Avoiding Repeated Computations with Caching

In this part of the cookbook, we need a more sizeable dataset than in the previous parts: dummy retail transactions, like the ones in Geni's [simple performance benchmark](../simple_performance_benchmark.md#dummy-retail-data), at half the size. As in every part, we start with Geni's core namespace:

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])
```

## 10.1 Generating the Data

The data has a million random transactions for each month of 2019, written to Parquet one month at a time. Generating it takes a while, so the code skips it when the data is there already:

```clojure
(def dummy-data-path "data/cookbook/dummy-retail")

(def max-days {1 31 2 28 3 31 4 30 5 31 6 30 7 31 8 31 9 30 10 31 11 30 12 31})

(defn transaction-id-col []
  (g/concat (g/str (g/random-int))
            (g/lit "-")
            (g/str (g/random-int))
            (g/lit "-")
            (g/str (g/random-int))))

(def date-col
  (g/concat :year (g/lit "-") :month (g/lit "-") :day))

(when-not (.exists (io/file dummy-data-path))
  (doseq [month (range 1 13)]
    (-> (g/range 1000000)
        (g/select
         {:trx-id    (transaction-id-col)
          :member-id (g/int (g/rexp 1e-5))
          :quantity  (g/int (g/inc (g/rexp)))
          :price     (g/pow 2 (g/random-int 16 20))
          :style-id  (g/int (g/rexp 1e-2))
          :brand-id  (g/int (g/rexp 1e-2))
          :year      2019
          :month     month
          :day       (g/random-int 1 (inc (max-days month)))})
        (g/with-column :date (g/to-date date-col))
        (g/coalesce 1)
        (g/write-parquet! dummy-data-path {:mode "append"}))))
```

We load and have a brief look at the data:

```clojure
(def transactions (g/read-parquet! dummy-data-path))

(g/count transactions)
;; => 12000000

(g/print-schema transactions)
;; =stdout=>
; root
;  |-- member-id: integer (nullable = true)
;  |-- day: long (nullable = true)
;  |-- trx-id: string (nullable = true)
;  |-- brand-id: integer (nullable = true)
;  |-- month: long (nullable = true)
;  |-- year: long (nullable = true)
;  |-- quantity: integer (nullable = true)
;  |-- price: double (nullable = true)
;  |-- style-id: integer (nullable = true)
;  |-- date: date (nullable = true)
```

The dataset is a table of dummy transactions that record the member/customer, the timing and the purchased goods: exactly 12 million transactions, a million for each month of the year.

## 10.2 Putting Together A Member Profile

Suppose that within a larger script, we've put together two different dataframes - one for summarising the members' spending behaviours and the other for summarising their visit frequencies:

```clojure
(def member-spending
  (-> transactions
      (g/with-column :sales (g/* :price :quantity))
      (g/group-by :member-id)
      (g/agg {:total-spend     (g/sum :sales)
              :avg-basket-size (g/mean :sales)
              :avg-price       (g/mean :price)})))

(def member-frequency
  (-> transactions
      (g/group-by :member-id)
      (g/agg {:n-transactions (g/count "*")
              :n-visits       (g/count-distinct :date)})))
```

In another part of the script, we would like to put together a customer profile that puts together their spending behaviours and visit frequencies:

```clojure
(def member-profile
  (g/join member-spending member-frequency :member-id))

(g/print-schema member-profile)
;; =stdout=>
; root
;  |-- member-id: integer (nullable = true)
;  |-- total-spend: double (nullable = true)
;  |-- avg-basket-size: double (nullable = true)
;  |-- avg-price: double (nullable = true)
;  |-- n-transactions: long (nullable = false)
;  |-- n-visits: long (nullable = false)
```

## 10.3 Caching Intermediate Results

At this point, the dataset `member-profile` is derived from several possibly expensive computational steps. Spark does not save the intermediate results unless specifically asked. So that if we were to use a dataset such as `member-profile` in further computations, Spark will re-do the two group-by operations and the one join operation. This means that we can potentially save time by telling Spark which datasets to cache using `g/cache`.

To illustrate this effect, let's suppose the dataset `member-profile` is used in five other computations, which we replace with a dummy `g/write-parquet!` operation over a loop. The timings below come from one run with two local Spark cores, and yours will differ:

```clojure
(defn some-other-computations [member-profile]
  (g/write-parquet! member-profile "data/cookbook/member-profile.parquet" {:mode "overwrite"}))

(doall (for [_ (range 5)]
         (time (some-other-computations member-profile))))
; "Elapsed time: 9285.895963 msecs"
; "Elapsed time: 8429.818504 msecs"
; "Elapsed time: 7881.109296 msecs"
; "Elapsed time: 7884.581796 msecs"
; "Elapsed time: 7917.635921 msecs"
```

Each step redid the expensive computations. However, if we had cached the dataset, we would take a hit on the first step, but the next steps would use the saved intermediate computations:

```clojure
(def cached-member-profile
  (g/cache member-profile))

(doall (for [_ (range 5)]
         (time (some-other-computations cached-member-profile))))
; "Elapsed time: 9770.506671 msecs"
; "Elapsed time: 1126.209834 msecs"
; "Elapsed time: 1135.704625 msecs"
; "Elapsed time: 1145.360542 msecs"
; "Elapsed time: 1125.059167 msecs"
```

The first run with the cache took a little longer than the runs without it, since it also saved the profile, and the runs after it were about seven times faster.

## 10.4 Further Resources

To understand when the intermediate computations are triggered and saved, we must first distinguish between Spark actions and transformations. For instance, this [blog article](https://medium.com/@aristo_alex/how-apache-sparks-transformations-and-action-works-ceb0d03b00d0) discusses Spark RDD actions and transformations, which work the same way as Spark datasets.

Furthermore, `g/cache`, by default, caches to memory and disk. However, Spark provides more fine-grained control over where to cache the intermediate computations using `g/persist`. For instance, `(g/persist dataframe g/memory-only)` forces a memory-only cache. See this [blog article](https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/) for a slightly more detailed treatment.
