# CB10: Avoiding Repeated Computations with Caching

In this part of the cookbook, we will require a more sizeable dataset than previous parts. In particular, we will be using [the dummy retail data](https://github.com/zero-one-group/geni/blob/develop/docs/simple_performance_benchmark.md#dummy-retail-data) used in Geni's simple performance benchmark doc. To generate the data locally, simply copy and paste [the data-generation code](https://github.com/zero-one-group/geni/blob/develop/examples/performance_benchmark_data.clj) to your Geni REPL. We assume that the data is stored in `/data/performance_benchmark_data` directory, but it does not need to be.

We load and have a brief look at the data:

```clojure
(def dummy-data-path "/data/performance-benchmark-data")

(def transactions (g/read-parquet! dummy-data-path))

(g/count transactions)
=> 24000000

(g/print-schema transactions)
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

The dataset is a table of dummy transactions that record the member/customer, the timing and the purchased goods. There are exactly 24 million transactions (exactly two million per month for every month in a year), and approximately one million members.

## 10.1 Putting Together A Member Profile

Suppose that within a larger script, we've put together two different dataframes - one for summarising the members' spending behaviours and the other for sumarising their visit frequencies:

```clojure
...

(def member-spending
  (-> transactions
      (g/with-column :sales (g/* :price :quantity))
      (g/group-by :member-id)
      (g/agg {:total-spend     (g/sum :sales)
              :avg-basket-size (g/mean :sales)
              :avg-price       (g/mean :price)})))

...

(def member-frequency
  (-> transactions
      (g/group-by :member-id)
      (g/agg {:n-transactions (g/count "*")
              :n-visits       (g/count-distinct :date)})))

...
```

In another part of the script, we would like to put together a customer profile that puts together their spending behaviours and visit ferquencies:

```clojure
(def member-profile
  (g/join member-spending member-frequency :member-id))

(g/print-schema member-profile)
; root
;  |-- member-id: integer (nullable = true)
;  |-- total-spend: double (nullable = true)
;  |-- avg-basket-size: double (nullable = true)
;  |-- avg-price: double (nullable = true)
;  |-- n-transactions: long (nullable = false)
;  |-- n-visits: long (nullable = false)
```

## 10.2 Caching Intermediate Results

At this point, the dataset `member-profile` is derived from several possibly expensive computational steps. One thing we must bear in mind when working with Spark datasets is that Spark won't save (some) intermediate results unless specifically asked. So that if we were to use a dataset such as `member-profile` in further computations, Spark will re-do the two group-by operations and the one join operation. This means that we can potentially save time by telling Spark which datasets to cache using `g/cache`.

To illustrate this effect, let's suppose the dataset `member-profile` is used in five other computations, which we replace with a dummy `g/write-parquet!` operation over a loop:

```clojure
(defn some-other-computations [member-profile]
  (g/write-parquet! member-profile "data/temp.parquet" {:mode "overwrite"}))

(doall (for [_ (range 5)]
         (time (some-other-computations member-profile))))
; "Elapsed time: 10083.047244 msecs"
; "Elapsed time: 8231.45662 msecs"
; "Elapsed time: 8525.947692 msecs"
; "Elapsed time: 8155.982435 msecs"
; "Elapsed time: 7638.144858 msecs"
```

Each step took 7-10 seconds, as Spark re-did some of the expensive computations. However, if we had cached the dataset, we would take a hit on the first step, but the next steps would use the saved intermediate computations:

```clojure
(def cached-member-profile
  (g/cache member-profile))

(doall (for [_ (range 5)]
         (time (some-other-computations cached-member-profile))))
; "Elapsed time: 11996.307581 msecs"
; "Elapsed time: 988.958567 msecs"
; "Elapsed time: 1017.365143 msecs"
; "Elapsed time: 1032.578846 msecs"
; "Elapsed time: 1087.077004 msecs"
```

We can see that the first time the computation ran, it took 12 seconds (i.e. around 20-50% slower than before), but the subsequent steps were 10x faster.

## 10.3 Further Resources

To understand when the intermediate computations are triggered and saved, we must first distinguish between Spark actions and transformations. For instance, this [blog article](https://medium.com/@aristo_alex/how-apache-sparks-transformations-and-action-works-ceb0d03b00d0) discusses Spark RDD actions and transformations, which work the same way as Spark datasets.

Furthermore, `g/cache`, by default, caches to memory and disk. However, Spark provides more fine-grained control over where to cache the intermediate computations using `g/persist`. For instance, `(g/persist dataframe g/memory-only)` forces a memory-only cache. See this [blog article](https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/) for a slightly more detailed treatment.
